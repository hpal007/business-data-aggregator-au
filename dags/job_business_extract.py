import requests
import json
import re
from bs4 import BeautifulSoup
from bs4.builder import XMLParsedAsHTMLWarning
import warnings
from warcio.archiveiterator import ArchiveIterator
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import threading
import time

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

USER_AGENT = "Mozilla/5.0 (compatible; ArchiveBot/1.0; +https://www.commoncrawl.org/faq/#how-do-you-crawl)"

# Thread-local storage for session (one per worker thread)
_thread_local = threading.local()

# NEW: Track request times to implement rate limiting
_request_times = []
_request_lock = threading.Lock()

# NEW: Configuration for rate limiting
MAX_REQUESTS_PER_SECOND = 13  # Limit to 2 requests per second per worker
MIN_DELAY_BETWEEN_REQUESTS = 1.0 / MAX_REQUESTS_PER_SECOND  # 0.5 seconds


def rate_limit_wait():
    """
    NEW: Implement rate limiting to avoid 403 errors from Common Crawl.
    Ensures we don't exceed MAX_REQUESTS_PER_SECOND
    """
    global _request_times

    with _request_lock:
        current_time = time.time()

        # Remove old request times (older than 1 second)
        _request_times = [t for t in _request_times if current_time - t < 1.0]

        # If we've exceeded the limit, wait
        if len(_request_times) >= MAX_REQUESTS_PER_SECOND:
            sleep_time = 1.0 - (current_time - _request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Record this request
        _request_times.append(time.time())


def get_session():
    """
    Get or create a requests session with connection pooling and smart retry strategy.
    OPTIMIZED: Better retry strategy for Common Crawl rate limiting
    """
    if not hasattr(_thread_local, "session"):
        session = requests.Session()

        # NEW: Custom retry strategy that handles 403 errors
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[
                429,
                403,
                503,
                504,
            ],  # CHANGED: Added 403 to retryable status codes
            allowed_methods=["GET"],  # Only retry GET requests
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # CHANGED: Reduced from 20 to 5 (less aggressive)
            pool_maxsize=10,  # CHANGED: Reduced from 20 to 5 (less aggressive)
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # NEW: Set better timeout
        session.timeout = 10

        _thread_local.session = session

    return _thread_local.session


# === Improved Regex Definitions ===
PHONE_RE = re.compile(
    r"""
    (?:^|[\s(])
    (?:
        \+61\s?[2-478]\s?\d{4}\s?\d{4}
        |
        \(0[2-478]\)\s?\d{4}\s?\d{4}
        |
        0[2-478]\s?\d{4}\s?\d{4}
        |
        1[38]00\s?\d{3}\s?\d{3}
        |
        04\d{2}\s?\d{3}\s?\d{3}
    )
    (?=[\s),.;]|$)
    """,
    re.VERBOSE | re.IGNORECASE,
)

PTY_LTD_RE = re.compile(
    r"\b([A-Z][A-Za-z0-9&'\-]{1,40}(?:\s+[A-Z][A-Za-z0-9&'\-]{1,40}){0,3})\s+(Pty\.?\s*Ltd\.?|PTY\s*LTD|Limited|LLC|Inc\.?|Corporation)\b",
    re.IGNORECASE,
)

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9][A-Za-z0-9._%+\-]{0,63}@[A-Za-z0-9][A-Za-z0-9.\-]{0,253}\.[A-Za-z]{2,}\b",
    re.IGNORECASE,
)

ABN_RE = re.compile(r"\bABN[:\s]*([0-9\s]{11,14})\b", re.IGNORECASE)

EXCLUDE_PHONE_PATTERNS = [
    r"^\d{1,2}\.\d{4}",
    r"^\d{4}[-/.]\d{2,4}",
    r"^\d{1,4}[-/]\d{1,4}$",
    r"^\d+\.\d+$",
    r"^\d{2,4}\s\d{3,4}$",
    r"^\d\s\d\s\d",
    r"^\d{2,3}\s\d{3}\s\d{3}\s\d{3}$",
]

STATE_TOKENS = ["vic", "nsw", "qld", "wa", "sa", "tas", "act", "nt"]
STATE_REGEX = re.compile(
    r"\b(" + r"|".join(re.escape(t) for t in STATE_TOKENS) + r")\b", re.IGNORECASE
)

ADDRESS_MARKERS = re.compile(
    r"\b(street|st|road|rd|avenue|ave|drive|dr|lane|ln|place|pl|court|ct|level|building|suite|unit|reply\s+paid)\b",
    re.IGNORECASE,
)

POSTCODE_RE = re.compile(r"\b\d{4}\b")
IGNORED_PARENT_TAGS = {"script", "style", "noscript", "meta", "link"}


def fetch_page_from_cc(record, myagent=USER_AGENT):
    """
    Fetch page from Common Crawl with intelligent rate limiting and backoff.
    OPTIMIZED: Handles 403 rate limiting with exponential backoff
    Returns tuple: (soup, error_info)
    """
    error_info = {"status": "success", "error": None}

    try:
        offset = int(record["warc_record_offset"])
        length = int(record["warc_record_length"])
        filename = record["warc_filename"]
        s3_url = f"https://data.commoncrawl.org/{filename}"

        byte_range = f"bytes={offset}-{offset + length - 1}"

        # NEW: Apply rate limiting before making request
        rate_limit_wait()

        # Use connection pooling session
        session = get_session()

        # Make request with exponential backoff on failure
        attempt = 0
        max_attempts = 5

        while attempt < max_attempts:
            try:
                response = session.get(
                    s3_url,
                    headers={
                        "user-agent": myagent,
                        "Range": byte_range,
                        "Accept": "*/*",  # NEW: Better headers for Common Crawl
                    },
                    stream=True,
                    timeout=15,  # CHANGED: Increased from 6 to 15 seconds
                    allow_redirects=True,  # NEW: Allow redirects
                )
                break  # Success, exit retry loop

            except requests.exceptions.Timeout:
                attempt += 1
                if attempt >= max_attempts:
                    error_info["status"] = "timeout"
                    error_info["error"] = (
                        f"Request timed out after {max_attempts} attempts"
                    )
                    return None, error_info
                # Exponential backoff before retry
                wait_time = 2**attempt
                time.sleep(wait_time)

            except requests.exceptions.ConnectionError:
                attempt += 1
                if attempt >= max_attempts:
                    error_info["status"] = "connection_error"
                    error_info["error"] = "Connection failed"
                    return None, error_info
                wait_time = 2**attempt
                time.sleep(wait_time)

            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt >= max_attempts:
                    error_info["status"] = "request_error"
                    error_info["error"] = str(e)[:50]
                    return None, error_info
                wait_time = 2**attempt
                time.sleep(wait_time)

        # Check response status
        if response.status_code == 403:
            error_info["status"] = "rate_limited"
            error_info["error"] = "Rate limited by Common Crawl (403)"
            return None, error_info
        elif response.status_code == 404:
            error_info["status"] = "not_found"
            error_info["error"] = "WARC record not found"
            return None, error_info
        elif response.status_code != 206:
            error_info["status"] = "invalid_status"
            error_info["error"] = f"Status {response.status_code}"
            return None, error_info

        # Parse WARC record
        try:
            stream = ArchiveIterator(response.raw)
            for warc_record in stream:
                if warc_record.rec_type == "response":
                    html_bytes = warc_record.content_stream().read()

                    if not html_bytes:
                        error_info["status"] = "empty_content"
                        error_info["error"] = "No content"
                        return None, error_info

                    html_text = html_bytes.decode("utf-8", errors="ignore")

                    if len(html_text) < 50:
                        error_info["status"] = "empty_html"
                        error_info["error"] = "Content too short"
                        return None, error_info

                    soup = BeautifulSoup(html_text, "html.parser")
                    return soup, error_info

            error_info["status"] = "no_response_record"
            error_info["error"] = "No response found"
            return None, error_info

        except Exception as e:
            error_info["status"] = "warc_parse_error"
            error_info["error"] = str(e)[:50]
            return None, error_info

    except ValueError as e:
        error_info["status"] = "invalid_record"
        error_info["error"] = str(e)[:50]
        return None, error_info
    except Exception as e:
        error_info["status"] = "unknown_error"
        error_info["error"] = str(e)[:50]
        return None, error_info


def normalize_phone(phone_str):
    """Normalize and validate Australian phone numbers"""
    original = phone_str.strip()

    for pattern in EXCLUDE_PHONE_PATTERNS:
        if re.match(pattern, original):
            return None

    if "." in original or original.count("-") > 2:
        return None

    if re.match(r"^\d{4}[-/]\d{4}$", original):
        return None

    if re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}", original):
        return None

    cleaned = re.sub(r"[^\d+]", "", original)

    if len(cleaned) == 11 and re.match(r"^\d{2}\s\d{3}\s\d{3}\s\d{3}$", original):
        return None

    if cleaned.startswith("+61"):
        if len(cleaned) != 12:
            return None
    elif cleaned.startswith("0"):
        if len(cleaned) != 10:
            return None
    elif cleaned.startswith("1"):
        if len(cleaned) != 10:
            return None
    else:
        return None

    digits_only = cleaned.replace("+", "").replace("61", "", 1)
    if len(set(digits_only)) == 1:
        return None

    return original


def normalize_abn(number_str):
    """Remove spaces from ABN"""
    return re.sub(r"\s+", "", number_str)


def is_likely_company_name(text, parent_tag):
    """Filter out obvious false positives for company names"""
    if len(text) < 5 or len(text) > 80:
        return False

    if any(char in text for char in [".", "?", "!"]):
        word_count = len(text.split())
        if word_count > 8:
            return False

    false_positive_keywords = [
        "click here",
        "read more",
        "learn more",
        "find out",
        "contact us",
        "about us",
        "terms and conditions",
        "privacy policy",
        "copyright",
    ]

    text_lower = text.lower()
    if any(kw in text_lower for kw in false_positive_keywords):
        return False

    if parent_tag and parent_tag.name in {"a", "button", "nav", "footer", "header"}:
        return False

    return True


def is_likely_address(text):
    """Check if text looks like a real address"""
    text_lower = text.lower()

    if not STATE_REGEX.search(text_lower):
        return False

    generic_phrases = [
        "life in",
        "living in",
        "visit",
        "located in",
        "based in",
        "welcome to",
        "your local",
        "trusted resource",
    ]

    if any(phrase in text_lower for phrase in generic_phrases):
        return False

    if text.startswith("http") or "://" in text:
        return False

    if len(text) < 15 or len(text) > 200:
        return False

    has_marker = ADDRESS_MARKERS.search(text_lower) is not None
    has_numbers = any(char.isdigit() for char in text)
    has_postcode = POSTCODE_RE.search(text) is not None

    if has_marker or (has_numbers and has_postcode and len(text) <= 100):
        return True

    return False


def extract_business_info_from_soup(soup, error_info=None):
    """Optimized extraction with early exit"""
    if soup is None:
        if error_info:
            return {
                "fetch_status": error_info["status"],
                "fetch_error": error_info["error"],
            }
        return {"fetch_status": "failed", "fetch_error": "Unknown error"}

    results = {
        "fetch_status": "success",
        "ABN": [],
        "CompanyName": [],
        "Emails": [],
        "Phones": [],
        "Addresses": [],
    }
    seen = {key: set() for key in results.keys()}

    MAX_RESULTS_PER_TYPE = 3

    # Only get text from footer and header
    footer_header_elements = soup.find_all(["footer", "header"])
    text_nodes = []

    for element in footer_header_elements:
        text_nodes.extend(element.find_all(string=True))

    for text_node in text_nodes:
        txt = text_node.strip()
        if not txt or len(txt) > 500:
            continue

        parent = text_node.parent
        if parent and parent.name and parent.name.lower() in IGNORED_PARENT_TAGS:
            continue

        if all(
            len(results[k]) >= MAX_RESULTS_PER_TYPE
            for k in results.keys()
            if k != "fetch_status"
        ):
            break

        for m in ABN_RE.finditer(txt):
            normalized = normalize_abn(m.group(1))
            if len(normalized) == 11 and normalized not in seen["ABN"]:
                seen["ABN"].add(normalized)
                results["ABN"].append({"matched_text": normalized})

        for em in EMAIL_RE.findall(txt):
            if em not in seen["Emails"]:
                seen["Emails"].add(em)
                results["Emails"].append({"matched_text": em})

        for m in PHONE_RE.finditer(txt):
            phone = normalize_phone(m.group(0))
            if phone and phone not in seen["Phones"]:
                seen["Phones"].add(phone)
                results["Phones"].append({"matched_text": phone})

        if is_likely_address(txt):
            if txt not in seen["Addresses"]:
                seen["Addresses"].add(txt)
                results["Addresses"].append({"matched_text": txt})

        for com in PTY_LTD_RE.finditer(txt):
            company = com.group(0).strip()
            if (
                is_likely_company_name(company, parent)
                and company not in seen["CompanyName"]
            ):
                seen["CompanyName"].add(company)
                results["CompanyName"].append({"matched_text": company})

    return results


def extract_json_fields(business_info_json_str):
    if business_info_json_str is None:
        return None, None, None
    try:
        data = json.loads(business_info_json_str)
        cc_abn = data["ABN"][0]["matched_text"] if data.get("ABN") else None
        company_name = (
            data["CompanyName"][0]["matched_text"] if data.get("CompanyName") else None
        )
        return cc_abn, company_name
    except Exception:
        return None, None, None


def process_partition(iterator):
    """
    Process a partition of records with intelligent rate limiting.
    Each partition respects Common Crawl rate limits.

    FIXED: Don't serialize BeautifulSoup objects - convert to string instead
    """
    for row in iterator:
        try:
            record = {
                "warc_record_offset": row.warc_record_offset,
                "warc_record_length": row.warc_record_length,
                "warc_filename": row.warc_filename,
                "url": row.url,
            }

            soup, error_info = fetch_page_from_cc(record)
            info = extract_business_info_from_soup(soup, error_info)
            business_info = json.dumps(info, ensure_ascii=False)
            cc_abn, company_name = extract_json_fields(business_info)

            raw_text_body = None
            if soup:
                body = soup.find("body")
                footer = soup.find("footer")
                if body:
                    # Remove script and style tags inside body first
                    for script in body(["script", "style"]):
                        script.decompose()
                    # Get cleaned text from body and replace \u0000
                    body_text = body.get_text(separator=" ", strip=True).replace(
                        "\u0000", ""
                    )
                else:
                    body_text = ""
                if footer:
                    # Remove script and style tags inside footer if any
                    for script in footer(["script", "style"]):
                        script.decompose()
                    # Get cleaned text from footer and replace \u0000
                    footer_text = footer.get_text(separator=" ", strip=True).replace(
                        "\u0000", ""
                    )
                else:
                    footer_text = ""
                raw_text_body = {"body": body_text, "footer": footer_text}

            yield (
                row.url,
                row.url_host_tld,
                row.url_host_registered_domain,
                row.fetch_status,
                row.content_mime_detected,
                row.content_mime_type,
                row.warc_filename,
                row.warc_record_offset,
                row.warc_record_length,
                cc_abn,
                company_name,
                business_info,
                raw_text_body,
            )
        except Exception as e:
            error_json = json.dumps(
                {
                    "fetch_status": "processing_error",
                    "fetch_error": str(e)[:100],
                },
                ensure_ascii=False,
            )
            yield (
                row.url,
                row.url_host_tld,
                row.url_host_registered_domain,
                row.fetch_status,
                row.content_mime_detected,
                row.content_mime_type,
                row.warc_filename,
                row.warc_record_offset,
                row.warc_record_length,
                None,
                None,
                error_json,
                None,
            )
