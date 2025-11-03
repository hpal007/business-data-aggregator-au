import requests
import gzip
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
import fsspec
import concurrent.futures
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

INDEX_NAME = 'CC-MAIN-2025-13'
BASE_URL = 'https://data.commoncrawl.org'

def get_parquet_files(index_name):
    """Get list of parquet files from cc-index-table.paths.gz"""
    paths_url = f'{BASE_URL}/crawl-data/{index_name}/cc-index-table.paths.gz'
    logger.info(f"Fetching parquet file list from: {paths_url}")

    try:
        response = requests.get(paths_url, timeout=30)
        response.raise_for_status()

        with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
            content = gz.read().decode('utf-8')

        parquet_files = [line.strip() for line in content.split('\n') if line.strip()]
        logger.info(f"Found {len(parquet_files)} parquet files")
        return parquet_files

    except requests.RequestException as e:
        logger.error(f"Failed to fetch parquet list: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return []

def read_table_from_url(url):
    """Read parquet table from URL with error handling"""
    try:
        full_url = f"{BASE_URL}/{url}"
        with fsspec.open(full_url, 'rb') as f:
            return pq.read_table(f, columns=['url_host_tld', 'url_host_registered_domain','url',
                                             'warc_filename', 'warc_record_offset', 'warc_record_length'])
    except Exception as e:
        logger.warning(f"Failed to read {url}: {e}")
        return None

def filter_and_combine_au(tables):
    """Filter tables for AU domain and combine"""
    filtered_chunks = []
    failed_count = 0

    for tbl in tables:
        if tbl is None:
            failed_count += 1
            continue

        mask = pc.equal(tbl['url_host_tld'], 'au')
        filtered_tbl = tbl.filter(mask)
        if filtered_tbl.num_rows > 0:
            filtered_chunks.append(filtered_tbl)

    if failed_count > 0:
        logger.warning(f"Skipped {failed_count} failed tables")

    if filtered_chunks:
        result = pa.concat_tables(filtered_chunks)
        logger.info(f"Concatenated {len(filtered_chunks)} tables with AU records")
    else:
        logger.warning("No AU records found")
        result = pa.table({'url_host_tld': pa.array([]), 'url_host_registered_domain': pa.array([]),
                           'warc_filename': pa.array([]), 'warc_record_offset': pa.array([]),
                           'warc_record_length': pa.array([])})

    return result

if __name__ == "__main__":
    logger.info(f"Starting AU domain extraction for index: {INDEX_NAME}")

    parquet_files = get_parquet_files(INDEX_NAME)
    if not parquet_files:
        logger.error("No parquet files found. Exiting.")
        exit(1)

    limit = min(100, len(parquet_files))
    logger.info(f"Processing {limit} parquet files")

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        tables = list(executor.map(read_table_from_url, parquet_files[:limit]))

    au_table = filter_and_combine_au(tables)
    logger.info(f"Total AU records found: {au_table.num_rows}")

    end_time = time.time()
    logger.info(f"Completed in {end_time - start_time:.2f} seconds")
    
    # Save to CSV
    output_file = 'au_domains1.csv'
    df_au = au_table.to_pandas()
    duplicates = len(df_au) - len(df_au.drop_duplicates())

    df_au.drop_duplicates().to_csv(output_file, index=False)
    logger.info(f"Saved {df_au.drop_duplicates().shape[0]} unique domains to {output_file} (removed {duplicates} duplicates)")


