# Company Name Matching Logic

This document explains the logic behind the company name matching process implemented in `dags/job_matching.py`.

## Overview

The goal of the matching job is to link company records from the **Common Crawl (CC)** dataset with the **Australian Business Register (ABR)** dataset. While the primary matching mechanism in the pipeline is based on the Australian Business Number (ABN), this job provides an additional layer of matching based on **Company Names**.

## Data Sources

1.  **Common Crawl (`cc_table`)**: Contains business information extracted from Australian websites, including `company_name` and `url`.
2.  **Australian Business Register (`abr_table`)**: Contains official business records, including `entity_name`, `abn`, and `entity_state`.

## Matching Strategy

The current implementation uses an **Exact Match on Normalized Names** strategy. This approach was chosen to provide high-precision matches without the computational overhead of a full cross-join fuzzy match on large datasets.

### 1. Normalization

Before matching, company names from both datasets are normalized to ensure consistency. The normalization process involves:
-   **Lowercasing**: Converting all characters to lowercase.
-   **Trimming**: Removing leading and trailing whitespace.

```python
# Example
"Acme Corp Pty Ltd "  -> "acme corp pty ltd"
"ACME CORP"           -> "acme corp"
```

*Note: The `clean_name` function in `dags/job_business_extract.py` performs more aggressive cleaning (removing suffixes like "Pty Ltd") during the extraction phase. The matching job relies on this pre-cleaned data where applicable, but applies its own basic normalization for safety.*

### 2. Exact Matching

The job performs an inner join between the normalized Common Crawl names and the normalized ABR entity names.

```sql
SELECT 
    cc.url,
    cc.company_name,
    abr.abn,
    abr.entity_name,
    abr.entity_state
FROM cc_table cc
JOIN abr_table abr ON normalized(cc.company_name) = normalized(abr.entity_name)
```

### 3. Output

The matched records are saved to a new PostgreSQL table named `matched_companies`.

**Schema:**
-   `cc_url`: URL of the website from Common Crawl.
-   `cc_company_name`: Company name extracted from the website.
-   `abr_abn`: Matched Australian Business Number.
-   `abr_entity_name`: Official entity name from ABR.
-   `entity_state`: State of the business (e.g., NSW, VIC).
-   `match_score`: Confidence score (currently `100` for exact matches).
-   `match_method`: Method used for matching (currently `"exact_name"`).

## Future Improvements

-   **Fuzzy Matching**: Implementing algorithms like Levenshtein distance or MinHashLSH to find similar but not identical names (e.g., "Acme Corp" vs "Acme Corporation").
-   **Blocking**: Using `entity_state` or postcode as a blocking key to reduce the search space for fuzzy matching.
