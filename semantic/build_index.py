import os
import gzip
import json
import argparse
import logging
import requests
from typing import Dict, Any, List

from embed import embed_texts
from store import save_datasets

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CATALOG_URL = "https://open.canada.ca/static/od-do-canada.jsonl.gz"
LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_GZ_PATH = os.path.join(LOCAL_DIR, "od-do-canada.jsonl.gz")

# Formats worth indexing for discovery: tabular (queryable via SQL/datastore)
# plus PDF/TXT (readable via read_pdf / plain fetch).
TABULAR_FORMATS = {"CSV", "XLSX", "XLS", "PARQUET", "JSON", "PDF", "TXT"}

def download_catalog() -> None:
    """Download the full government catalog dump if it doesn't already exist locally."""
    if os.path.exists(LOCAL_GZ_PATH):
        logger.info(f"Catalog archive found locally at '{LOCAL_GZ_PATH}'. Skipping download.")
        return
        
    logger.info(f"Downloading catalog archive from {CATALOG_URL}...")
    response = requests.get(CATALOG_URL, stream=True, timeout=60)
    response.raise_for_status()
    
    with open(LOCAL_GZ_PATH, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    logger.info("Download completed successfully.")

def _get_english(val: Any) -> str:
    """Safely extract English content from bilingual CKAN dicts or pipe-separated strings."""
    if not val:
        return ""
    if isinstance(val, dict):
        # Prefer English, fallback to French or whatever is first
        return val.get("en") or val.get("fr") or next(iter(val.values())) or ""
    if isinstance(val, str):
        if "|" in val:
            # Governments separate bilingual texts with ' | ' e.g., 'English | Français'
            return val.split("|")[0].strip()
        return val.strip()
    return str(val)

def _get_english_list(val: Any) -> List[str]:
    """Safely extract a list of English words from bilingual lists or dictionaries."""
    if not val:
        return []
    if isinstance(val, list):
        return [_get_english(x) for x in val if x]
    if isinstance(val, dict):
        items = val.get("en") or val.get("fr") or next(iter(val.values())) or []
        if isinstance(items, list):
            return [_get_english(x) for x in items if x]
        elif isinstance(items, str):
            return [_get_english(items)]
    return []

def process_dataset_dict(ds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a parsed dataset dictionary and extract queryable metadata.
    Returns None if the dataset does not contain queryable tabular resources.
    """
    # Check resources first
    resources = ds.get("resources", [])
    if not resources or not isinstance(resources, list):
        return None
        
    # Filter for tabular formats
    has_tabular = False
    extracted_resources = []
    for r in resources:
        fmt = str(r.get("format", "")).strip().upper()
        if fmt in TABULAR_FORMATS:
            has_tabular = True
        extracted_resources.append({
            "id": r.get("id", ""),
            "name": _get_english(r.get("name")),
            "format": fmt,
            "url": r.get("url", ""),
            "datastore_active": bool(r.get("datastore_active", False))
        })
        
    if not has_tabular:
        return None
        
    # Extract metadata fields
    ds_id = ds.get("id")
    if not ds_id:
        return None
        
    title = _get_english(ds.get("title_translated") or ds.get("title") or ds.get("name"))
    notes = _get_english(ds.get("notes_translated") or ds.get("notes"))
    org = _get_english((ds.get("organization") or {}).get("title_translated") or (ds.get("organization") or {}).get("title"))
    
    # Extract topic categories/keywords
    topic = _get_english(ds.get("topic_category") or ds.get("subject"))
    keywords = _get_english_list(ds.get("keywords"))
    
    metadata_modified = ds.get("metadata_modified", "")
    
    # Compose clean English document text for embedding
    text_parts = []
    if title:
        text_parts.append(title)
    if notes:
        text_parts.append(notes)
    if keywords:
        text_parts.append(f"Keywords: {', '.join(keywords)}")
    if org:
        text_parts.append(f"Publisher: {org}")
    if topic:
        text_parts.append(f"Topic: {topic}")
    doc_text = "\n\n".join(text_parts)
    
    return {
        "id": ds_id,
        "title": title,
        "org": org,
        "notes": notes,
        "topic": topic,
        "resources": extracted_resources,
        "metadata_modified": metadata_modified,
        "doc_text": doc_text
    }

def process_dataset_line(line: str) -> Dict[str, Any]:
    """
    Parse a single line from the JSONL export and structure the dataset metadata.
    Returns None if the dataset does not contain queryable tabular resources.
    """
    try:
        ds = json.loads(line)
    except json.JSONDecodeError:
        return None
    return process_dataset_dict(ds)

def refresh_index(count: int) -> None:
    """
    Incrementally refresh the index by fetching recently modified packages from CKAN API.
    Does not require downloading the full catalogue dump.
    """
    logger.info(f"Querying Government of Canada portal for recently modified datasets (limit: {count})...")
    api_url = "https://open.canada.ca/data/en/api/3/action/package_search"
    params = {
        "q": "*:*",
        "sort": "metadata_modified desc",
        "rows": min(count, 1000)  # CKAN limit per search request
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=20)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            logger.error(f"CKAN search API failed: {result.get('error')}")
            return
    except Exception as e:
        logger.error(f"Failed to fetch updates from CKAN: {e}")
        return
        
    results = result.get("result", {}).get("results", [])
    logger.info(f"Fetched {len(results)} raw datasets from portal.")
    
    processed_datasets = []
    for ds in results:
        ds_data = process_dataset_dict(ds)
        if ds_data:
            processed_datasets.append(ds_data)
            
    logger.info(f"Filtered to {len(processed_datasets)} tabular datasets out of {len(results)} modified.")
    
    if not processed_datasets:
        logger.info("No recently modified datasets contain queryable tabular resources.")
        return
        
    # Generate embeddings
    texts = [ds["doc_text"] for ds in processed_datasets]
    logger.info(f"Generating embeddings for {len(texts)} updated datasets using local model...")
    embeddings = embed_texts(texts)
    
    for i, ds in enumerate(processed_datasets):
        ds["embedding"] = embeddings[i]
        
    logger.info(f"Upserting {len(processed_datasets)} records into DuckDB vector store...")
    save_datasets(processed_datasets)
    logger.info("Incremental refresh completed.")

def build_index(limit: int = None) -> None:
    """Read local archive, stream & parse records, embed them, and save to DuckDB."""
    download_catalog()
    
    logger.info("Parsing dataset catalog...")
    processed_datasets = []
    total_lines = 0
    
    with gzip.open(LOCAL_GZ_PATH, "rt", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            if total_lines % 5000 == 0:
                logger.info(f"Processed {total_lines} catalog lines...")
                
            ds_data = process_dataset_line(line)
            if ds_data:
                processed_datasets.append(ds_data)
                
            if limit and len(processed_datasets) >= limit:
                logger.info(f"Reached user-specified limit of {limit} datasets. Stopping parser.")
                break
                
    logger.info(f"Parser finished. Total records: {total_lines}. Kept with tabular resources: {len(processed_datasets)}.")
    
    if not processed_datasets:
        logger.warning("No datasets match the tabular filter criteria.")
        return
        
    # Extract doc texts to embed
    texts = [ds["doc_text"] for ds in processed_datasets]
    
    logger.info(f"Generating embeddings for {len(texts)} datasets using local model...")
    embeddings = embed_texts(texts, is_query=False)
    logger.info("Embedding generation finished.")
    
    # Map embeddings back to dataset objects
    for i, ds in enumerate(processed_datasets):
        ds["embedding"] = embeddings[i]
        
    logger.info("Saving records and vectors to DuckDB index...")
    save_datasets(processed_datasets)
    logger.info("Catalog indexing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build or refresh semantic search index over government datasets.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of datasets to index (for faster testing).")
    parser.add_argument("--refresh", type=int, default=None, help="Incrementally refresh index with the specified number of recently modified datasets.")
    args = parser.parse_args()
    
    if args.refresh is not None:
        refresh_index(args.refresh)
    else:
        build_index(limit=args.limit)
