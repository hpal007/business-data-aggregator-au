from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import requests
import json
import re
import os
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from pathlib import Path

USER_AGENT = "cc-get-started/1.0 (Example data retrieval script; yourname@example.com)"


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
ACN_RE = re.compile(r"\bACN[:\s]*([0-9\s]{9,11})\b", re.IGNORECASE)

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
    Fetch page from Common Crawl with timeout and error handling
    Returns tuple: (soup, error_info)
    """
    error_info = {"status": "success", "error": None}

    try:
        offset = int(record["warc_record_offset"])
        length = int(record["warc_record_length"])
        filename = record["warc_filename"]
        s3_url = f"https://data.commoncrawl.org/{filename}"

        byte_range = f"bytes={offset}-{offset + length - 1}"

        # Make request with detailed timeout handling
        try:
            response = requests.get(
                s3_url,
                headers={"user-agent": myagent, "Range": byte_range},
                stream=True,
                timeout=15,
            )
        except requests.exceptions.Timeout:
            error_info["status"] = "timeout"
            error_info["error"] = "Request timed out after 15 seconds"
            return None, error_info
        except requests.exceptions.ConnectionError:
            error_info["status"] = "connection_error"
            error_info["error"] = "Failed to establish connection"
            return None, error_info
        except requests.exceptions.RequestException as e:
            error_info["status"] = "request_error"
            error_info["error"] = f"Request failed: {str(e)}"
            return None, error_info

        # Check response status
        if response.status_code != 206:
            error_info["status"] = "invalid_status"
            error_info["error"] = f"Expected status 206, got {response.status_code}"
            return None, error_info

        # Parse WARC record
        try:
            stream = ArchiveIterator(response.raw)
            for warc_record in stream:
                if warc_record.rec_type == "response":
                    html_bytes = warc_record.content_stream().read()

                    # Check if we got any content
                    if not html_bytes:
                        error_info["status"] = "empty_content"
                        error_info["error"] = "No content received from WARC record"
                        return None, error_info

                    html_text = html_bytes.decode("utf-8", errors="ignore")
                    soup = BeautifulSoup(html_text, "html.parser")

                    # Verify soup has content
                    if not soup or len(soup.get_text(strip=True)) < 10:
                        error_info["status"] = "empty_html"
                        error_info["error"] = "HTML content too short or empty"
                        return None, error_info

                    return soup, error_info

            # No response record found in WARC
            error_info["status"] = "no_response_record"
            error_info["error"] = "No response record found in WARC stream"
            return None, error_info

        except Exception as e:
            error_info["status"] = "warc_parse_error"
            error_info["error"] = f"WARC parsing failed: {str(e)}"
            return None, error_info

    except ValueError as e:
        error_info["status"] = "invalid_record"
        error_info["error"] = f"Invalid record data: {str(e)}"
        return None, error_info
    except Exception as e:
        error_info["status"] = "unknown_error"
        error_info["error"] = f"Unexpected error: {str(e)}"
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


def normalize_abn_acn(number_str):
    """Remove spaces from ABN/ACN"""
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
        "all rights",
        "sign up",
        "log in",
        "register",
        "subscribe",
        "download",
        "cart is",
        "email address",
        "service provided by",
        "advertising service",
        "management and message",
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
        "progression to",
        "visit",
        "located in",
        "based in",
        "operating in",
        "serving",
        "around melbourne",
        "around sydney",
        "select a suburb",
        "choose from",
        "delivered on behalf",
        "welcome to",
        "your local",
        "trusted resource",
        "since 20",
        "discover the best",
        "trade services",
        "contact hours",
        "support contact",
        "public holidays",
        "monday",
        "tuesday",
        "partnership",
        "gov ",
        "hospital",
        "directory",
    ]

    if any(phrase in text_lower for phrase in generic_phrases):
        return False

    if text.startswith("http") or "://" in text:
        return False

    if any(ext in text_lower for ext in [".jpg", ".png", ".gif", ".jpeg"]):
        return False

    if len(text.split()) <= 3 and POSTCODE_RE.search(text):
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
    """Enhanced extraction with better filtering and error tracking"""
    # If fetch failed, return error information
    if soup is None:
        if error_info:
            return {
                "fetch_status": error_info["status"],
                "fetch_error": error_info["error"],
            }
        return {"fetch_status": "failed", "fetch_error": "Unknown fetch error"}

    results = {
        "fetch_status": "success",
        "ABN": [],
        "ACN": [],
        "CompanyName": [],
        "Emails": [],
        "Phones": [],
        "Addresses": [],
        "StructuredData": [],
    }
    seen = {key: set() for key in results.keys()}
    found_abns = set()

    for text_node in soup.find_all(string=True):
        if not text_node.strip():
            continue

        parent = text_node.parent
        if parent and parent.name and parent.name.lower() in IGNORED_PARENT_TAGS:
            continue

        txt = text_node.strip()

        if len(txt) > 500:
            continue

        for m in ABN_RE.finditer(txt):
            normalized = normalize_abn_acn(m.group(1))
            if len(normalized) == 11 and normalized not in seen["ABN"]:
                seen["ABN"].add(normalized)
                found_abns.add(normalized)
                results["ABN"].append({"matched_text": normalized})

        for m in ACN_RE.finditer(txt):
            normalized = normalize_abn_acn(m.group(1))
            if len(normalized) == 9 and normalized not in seen["ACN"]:
                seen["ACN"].add(normalized)
                results["ACN"].append({"matched_text": normalized})

        for m in PTY_LTD_RE.finditer(txt):
            company = m.group(0).strip()
            if (
                is_likely_company_name(company, parent)
                and company not in seen["CompanyName"]
            ):
                seen["CompanyName"].add(company)
                results["CompanyName"].append({"matched_text": company})

        for em in EMAIL_RE.findall(txt):
            if em not in seen["Emails"] and not em.endswith(
                (".png", ".jpg", ".gif", ".jpeg")
            ):
                seen["Emails"].add(em)
                results["Emails"].append({"matched_text": em})

        for m in PHONE_RE.finditer(txt):
            phone = normalize_phone(m.group(0))
            if phone:
                phone_digits = re.sub(r"[^\d]", "", phone)
                if phone_digits not in found_abns and phone not in seen["Phones"]:
                    seen["Phones"].add(phone)
                    results["Phones"].append({"matched_text": phone})

        if is_likely_address(txt):
            addr = txt.strip()
            if addr not in seen["Addresses"]:
                seen["Addresses"].add(addr)
                results["Addresses"].append({"matched_text": addr})

    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        orgs = []
        if isinstance(data, dict) and data.get("@type") == "Organization":
            orgs.append(data)
        elif isinstance(data, list):
            orgs = [
                item
                for item in data
                if isinstance(item, dict) and item.get("@type") == "Organization"
            ]

        for org in orgs:
            org_data = {
                "name": org.get("name"),
                "telephone": org.get("telephone"),
                "email": org.get("email"),
            }

            if "address" in org:
                addr = org["address"]
                if isinstance(addr, dict):
                    addr_parts = [
                        addr.get("streetAddress"),
                        addr.get("addressLocality"),
                        addr.get("addressRegion"),
                        addr.get("postalCode"),
                    ]
                    org_data["address"] = ", ".join(filter(None, addr_parts))
                elif isinstance(addr, str):
                    org_data["address"] = addr

            org_data = {k: v for k, v in org_data.items() if v}
            if org_data and org_data not in results["StructuredData"]:
                results["StructuredData"].append(org_data)

    # Remove empty categories (except fetch_status)
    # results = {k: v for k, v in results.items() if v or k == "fetch_status"}

    # Add flag if no business info found
    data_keys = set(results.keys()) - {"fetch_status", "fetch_error"}
    if not data_keys:
        results["info"] = "No business info found"

    return results


def extract_json_fields(business_info_json_str):
    if business_info_json_str is None:
        return None, None, None
    try:
        data = json.loads(business_info_json_str)
        ABN = (
            data["ABN"][0]["matched_text"]
            if data.get("ABN") and len(data["ABN"]) > 0
            else None
        )
        ACN = (
            data["ACN"][0]["matched_text"]
            if data.get("ACN") and len(data["ACN"]) > 0
            else None
        )
        CompanyName = (
            data["CompanyName"][0]["matched_text"]
            if data.get("CompanyName") and len(data["CompanyName"]) > 0
            else None
        )
        return ABN, ACN, CompanyName
    except Exception:
        return None, None, None


def process_partition(iterator):
    """
    Process a partition of records. This function is serializable and will be
    executed on Spark workers without pickling issues.
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
            ABN, ACN, CompanyName = extract_json_fields(business_info)

            # Yield all original columns plus business_info
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
                ABN,
                ACN,
                CompanyName,
                business_info,
            )
        except Exception as e:
            error_json = json.dumps(
                {
                    "fetch_status": "processing_error",
                    "fetch_error": f"Processing failed: {str(e)}",
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
                error_json,
            )
