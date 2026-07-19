import os
import json
import logging
from typing import List, Dict, Any
import duckdb

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Locate the database relative to this file (project root)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "catalog.duckdb")

def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize the datasets table schema in DuckDB."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id VARCHAR PRIMARY KEY,
            title VARCHAR,
            org VARCHAR,
            notes VARCHAR,
            topic VARCHAR,
            resources_json VARCHAR,
            metadata_modified VARCHAR,
            embedding FLOAT[384]
        );
    """)
    logger.info("DuckDB schema initialized.")

def save_datasets(datasets_data: List[Dict[str, Any]]) -> None:
    """
    Save a batch of dataset records and their embeddings to the database.
    Performs bulk insertion using transaction blocks.
    """
    if not datasets_data:
        return
        
    conn = duckdb.connect(DB_PATH)
    try:
        init_db(conn)
        conn.execute("BEGIN TRANSACTION;")
        
        # Prepare rows for insertion
        rows = []
        for ds in datasets_data:
            rows.append((
                ds["id"],
                ds["title"],
                ds["org"],
                ds["notes"],
                ds["topic"],
                json.dumps(ds.get("resources", [])),
                ds.get("metadata_modified", ""),
                ds["embedding"]
            ))
            
        # Perform bulk upsert
        conn.executemany("""
            INSERT OR REPLACE INTO datasets (id, title, org, notes, topic, resources_json, metadata_modified, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?::FLOAT[384]);
        """, rows)
        
        conn.execute("COMMIT;")
        logger.info(f"Successfully saved {len(datasets_data)} records to {DB_PATH}")
    except Exception as e:
        conn.execute("ROLLBACK;")
        logger.error(f"Error saving datasets: {e}")
        raise e
    finally:
        conn.close()

def top_k(query_vec: List[float], k: int = 8) -> List[Dict[str, Any]]:
    """
    Retrieve the top-K datasets closest to the query vector.
    Uses DuckDB's built-in array_cosine_distance.
    """
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"Catalog database not found at '{DB_PATH}'. "
            "Please run 'python semantic/build_index.py' to generate it."
        )
        
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        res = conn.execute("""
            SELECT id, title, org, notes, topic, resources_json, metadata_modified,
                   array_cosine_distance(embedding, ?::FLOAT[384]) AS dist
            FROM datasets
            ORDER BY dist ASC
            LIMIT ?
        """, (query_vec, k)).fetchall()
        
        results = []
        for row in res:
            results.append({
                "id": row[0],
                "title": row[1],
                "org": row[2],
                "notes": row[3],
                "topic": row[4],
                "resources": json.loads(row[5]) if row[5] else [],
                "metadata_modified": row[6],
                "distance": row[7]
            })
        return results
    finally:
        conn.close()

def get_by_ids(ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch multiple datasets by their IDs from the DuckDB store in a single query.
    Returns a dictionary mapping id -> dataset.
    """
    if not ids:
        return {}
    if not os.path.exists(DB_PATH):
        return {}
        
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        placeholders = ",".join(["?"] * len(ids))
        res = conn.execute(f"""
            SELECT id, title, org, notes, topic, resources_json, metadata_modified
            FROM datasets
            WHERE id IN ({placeholders})
        """, ids).fetchall()
        
        results = {}
        for row in res:
            results[row[0]] = {
                "id": row[0],
                "title": row[1],
                "org": row[2],
                "notes": row[3],
                "topic": row[4],
                "resources": json.loads(row[5]) if row[5] else [],
                "metadata_modified": row[6],
                "distance": None
            }
        return results
    finally:
        conn.close()
