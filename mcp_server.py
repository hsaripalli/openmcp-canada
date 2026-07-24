"""
OpenMCP Canada — open.canada.ca MCP server.

A focused interface over the Government of Canada Open Data portal (CKAN).
Discovery and querying go exclusively through the documented CKAN Action API
(https://open.canada.ca/data/en/api/3/action/, GET-only).

Flow:
    search_datasets(query)            -> find datasets (package_search)
    get_dataset(dataset_id)           -> list resources + which are queryable
    query_datastore(resource_id, ...) -> server-side filter/search (no download)
    query_remote_file(url, sql)       -> DuckDB fallback for non-datastore files

Datastore-backed resources (datastore_active: true) are queried server-side via
datastore_search — fast, no full download. Everything else falls back to DuckDB
streaming over the file URL.
"""

import io
import re
import json
import ssl
import zipfile
import concurrent.futures
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import duckdb
from mcp.server.fastmcp import FastMCP

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load .env so telemetry (and any other) config is picked up regardless of launcher
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

from semantic.embed import embed_texts
from semantic.store import top_k, DB_PATH, get_by_ids
from telemetry import log_telemetry, is_telemetry_disabled

mcp = FastMCP("OpenMCP Canada — open.canada.ca")

if is_telemetry_disabled():
    sys.stderr.write("[OpenMCP] Anonymous telemetry disabled via environment variable.\n")
else:
    sys.stderr.write("[OpenMCP] Anonymous telemetry active (opt out with OPENMCP_TELEMETRY_DISABLED=true).\n")


# ── CKAN Action API (open.canada.ca is GET-only; pass params in the URL) ───────
CKAN_BASE = "https://open.canada.ca/data/en/api/3/action"
HTTP_TIMEOUT = 20
QUERY_TIMEOUT = 30

MAX_PREVIEW_ROWS = 15
MAX_RESULT_ROWS = 100
MAX_CELL_CHARS = 200
MAX_DESC_CHARS = 300
GEO_COL_KEYWORDS = ("polygon", "geom", "wkt", "shape", "multipolygon", "coordinates")

# Persistent DuckDB connection (httpfs loaded once) for the file fallback path.
_duck = duckdb.connect(":memory:")
_duck.execute("INSTALL httpfs; LOAD httpfs;")

_WRITE_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|attach|copy|pragma|"
    r"create\s+table|create\s+or\s+replace)\b",
    re.IGNORECASE,
)


# ── robust remote file download ──────────────────────────────────────────────
# Some government hosts (statcan.gc.ca in particular) intermittently reset
# connections from clients with the default `python-requests/x.y` User-Agent,
# and some also mishandle TLS 1.3 negotiation. Send a browser-ish UA, retry
# transient errors, then fall back to a TLS-1.2-pinned connection.
_UA = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "OpenMCP-Canada/1.0")}
_retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
                status_forcelist=(500, 502, 503, 504))
_dl_session = requests.Session()
_dl_session.headers.update(_UA)
_dl_session.mount("https://", HTTPAdapter(max_retries=_retry))
_dl_session.mount("http://", HTTPAdapter(max_retries=_retry))

# Transient network failures worth retrying over TLS 1.2. ChunkedEncodingError
# is a RequestException but NOT a ConnectionError subclass — a premature body
# termination would otherwise escape the fallback.
_TRANSIENT_ERRORS = (requests.exceptions.ConnectionError,
                     requests.exceptions.SSLError,
                     requests.exceptions.ChunkedEncodingError)


class _TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


def _robust_get(url: str, timeout: int = None) -> requests.Response:
    """GET a remote file with retries; falls back to TLS 1.2 on handshake resets."""
    timeout = timeout or HTTP_TIMEOUT
    try:
        resp = _dl_session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp
    except _TRANSIENT_ERRORS:
        tls12 = requests.Session()
        tls12.headers.update(_UA)
        tls12.mount("https://", _TLS12Adapter(max_retries=_retry))
        resp = tls12.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp


# ── helpers ────────────────────────────────────────────────────────────────────
def _ckan_get(action: str, **params) -> Dict[str, Any]:
    """Call a CKAN Action API endpoint (GET) and return the `result` payload."""
    resp = _dl_session.get(f"{CKAN_BASE}/{action}", params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"CKAN error on {action}: {body.get('error')}")
    return body.get("result", {})


def _extract_dataset_id(dataset_id: str) -> str:
    """Accept a bare id/slug or a full open.canada.ca dataset URL."""
    m = re.search(r"/dataset/([0-9a-f-]{36}|[\w-]+)", dataset_id)
    return m.group(1) if m else dataset_id.strip()


def _truncate_desc(text: str) -> str:
    """Strip the French half of bilingual notes and truncate."""
    if not text:
        return "No description available."
    if "|" in text:
        text = text.split("|")[0]
    text = " ".join(text.split())
    if len(text) > MAX_DESC_CHARS:
        text = text[:MAX_DESC_CHARS].rsplit(" ", 1)[0] + "…"
    return text


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Drop geometry columns, drop CKAN's internal _id, truncate long cells."""
    df = df.drop(columns=["_id", "_full_text"], errors="ignore")
    geo = [c for c in df.columns if any(k in c.lower() for k in GEO_COL_KEYWORDS)]
    df = df.drop(columns=geo, errors="ignore")
    return df.map(
        lambda x: (str(x)[:MAX_CELL_CHARS] + "…")
        if isinstance(x, str) and len(x) > MAX_CELL_CHARS else x
    )


def _df_to_md(df: pd.DataFrame, cap: int = MAX_RESULT_ROWS, offset: int = 0,
              total: Optional[int] = None) -> str:
    """Render a dataframe as markdown, capping rows and noting pagination."""
    shown_total = total if total is not None else len(df)
    df = df.head(cap).map(lambda x: str(x) if pd.notnull(x) else "")
    if df.empty:
        return "_0 rows._"
    md = df.to_markdown(index=False)
    if shown_total > offset + len(df):
        md += (f"\n\n_Showing rows {offset}–{offset + len(df)} of {shown_total}. "
               f"Pass offset={offset + cap} for the next page._")
    return md


def run_query_with_timeout(sql: str, timeout_sec: int = QUERY_TIMEOUT) -> pd.DataFrame:
    """Run a DuckDB query on the shared connection with a wall-clock timeout."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(lambda: _duck.execute(sql).fetchdf())
        try:
            return fut.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(
                f"Query timed out after {timeout_sec}s. If this resource has "
                f"datastore_active:true, use query_datastore instead (server-side)."
            )


# ── Excel workbook cache (url → (bytes, timestamp)) ──────────────────────────
# Multi-sheet workflows (list → preview a sheet → query a sheet) would otherwise
# re-download the whole workbook on every call. Cache the bytes briefly.
import time
_EXCEL_CACHE: Dict[str, Tuple[bytes, float]] = {}
_EXCEL_TTL = 1800        # 30 min
_EXCEL_CACHE_MAX = 8


def _excel_file(url: str) -> "pd.ExcelFile":
    """Return a pandas ExcelFile for a remote workbook, caching the raw bytes."""
    hit = _EXCEL_CACHE.get(url)
    if hit and time.time() - hit[1] < _EXCEL_TTL:
        data = hit[0]
    else:
        resp = _robust_get(url)
        data = resp.content
        if len(_EXCEL_CACHE) >= _EXCEL_CACHE_MAX:
            del _EXCEL_CACHE[min(_EXCEL_CACHE, key=lambda k: _EXCEL_CACHE[k][1])]
        _EXCEL_CACHE[url] = (data, time.time())
    return pd.ExcelFile(io.BytesIO(data))


def _detect_header_row(xl: "pd.ExcelFile", sheet_name: Any, max_scan: int = 20) -> int:
    """Scan the first max_scan rows and return the index of the most likely header row.

    Government Excel files often have a title or metadata block before the real table.
    We pick the row with the most non-empty string-valued cells — that's almost always
    the header row.  Falls back to 0 if nothing looks clearly better.
    """
    try:
        raw = xl.parse(sheet_name, header=None, nrows=max_scan)
    except Exception:
        return 0
    best_row, best_count = 0, 0
    for i, row in raw.iterrows():
        str_count = sum(
            1 for v in row if isinstance(v, str) and v.strip()
        )
        if str_count > best_count:
            best_count = str_count
            best_row = int(i)
    return best_row


def _query_dataframe(sql: str, df: pd.DataFrame,
                     timeout_sec: int = QUERY_TIMEOUT) -> pd.DataFrame:
    """Run read-only SQL against an in-memory DataFrame (registered as `df`)."""
    def _run() -> pd.DataFrame:
        con = duckdb.connect(":memory:")
        con.register("df", df)
        try:
            return con.execute(sql).fetchdf()
        finally:
            con.close()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        try:
            return ex.submit(_run).result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Query timed out after {timeout_sec}s.")


def _read_tabular(url: str, nrows: Optional[int] = None,
                  sheet_name: Any = 0,
                  header_row: Optional[int] = None) -> pd.DataFrame:
    """Read a remote CSV/JSON/Parquet/XLSX (incl. zipped CSV) into a DataFrame.

    For Excel, `sheet_name` selects the sheet (name or index; default first sheet).
    `header_row` overrides auto-detection; pass None (default) to auto-detect.
    """
    low = url.lower()
    kw = {"nrows": nrows} if nrows else {}

    if low.endswith(".zip"):
        resp = _robust_get(url)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            names = [n for n in zf.namelist()
                     if n.lower().endswith(".csv") and "__MACOSX" not in n]
            if not names:
                raise ValueError(f"No CSV inside ZIP: {url}")
            name = next((n for n in names if "-eng" in n.lower()), names[0])
            raw = zf.read(name)
        for enc in ("utf-8", "utf-16", "latin-1"):
            try:
                return pd.read_csv(io.BytesIO(raw), encoding=enc, **kw)
            except (UnicodeDecodeError, UnicodeError):
                continue
        return pd.read_csv(io.BytesIO(raw), encoding="latin-1", **kw)

    if low.endswith(".parquet"):
        df = pd.read_parquet(url)
        return df.head(nrows) if nrows else df

    if low.endswith((".xlsx", ".xls")):
        xl = _excel_file(url)
        hdr = header_row if header_row is not None else _detect_header_row(xl, sheet_name)
        df = xl.parse(sheet_name, header=hdr, **kw)
        # Strip unnamed carry-over columns that pandas generates for blank header cells
        df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed: \d+$")]
        return df

    if low.endswith(".json"):
        resp = _robust_get(url)
        df = pd.read_json(io.BytesIO(resp.content))
        return df.head(nrows) if nrows else df

    for enc in ("utf-8", "utf-16", "latin-1"):
        try:
            return pd.read_csv(url, encoding=enc, **kw)
        except (UnicodeDecodeError, UnicodeError):
            continue
    return pd.read_csv(url, encoding="latin-1", **kw)


# =====================================================================
# DISCOVERY
# =====================================================================
@mcp.tool()
@log_telemetry("semantic_search_datasets")
def semantic_search_datasets(query: str, limit: int = 10) -> str:
    """
    Find Government of Canada open datasets by semantic meaning or natural language questions.
    Uses Reciprocal Rank Fusion (RRF) to combine local semantic vector search (bge-small-en-v1.5,
    runs locally — no API key) with the live portal's keyword search for maximum accuracy.
    
    Examples:
        - "how much did municipalities spend on infrastructure?"
        - "population demographics of alberta cities"
        - "water quality testing records"
        
    Args:
        query: Plain English search term, acronym, or question.
        limit: Max datasets to return (default 8).
    """
    if not os.path.exists(DB_PATH):
        return (
            "Error: Semantic database catalog is missing.\n\n"
            "To use this tool, you must build the semantic search index first. "
            "Please run the following command in the project directory:\n"
            "```bash\n"
            "python semantic/build_index.py\n"
            "```"
        )
        
    # 1. Retrieve semantic search results (top 25)
    try:
        query_vecs = embed_texts([query], is_query=True)
        if not query_vecs:
            semantic_results = []
        else:
            semantic_results = top_k(query_vecs[0], k=25)
    except Exception as e:
        # Fall back to empty list if local DB search fails
        semantic_results = []
        
    # 2. Retrieve keyword search results (top 25) from CKAN API
    try:
        keyword_raw = _ckan_get("package_search", q=query, rows=25)
        keyword_results = keyword_raw.get("results", [])
    except Exception:
        # Fall back to empty list if CKAN API is unreachable
        keyword_results = []
        
    if not semantic_results and not keyword_results:
        return f"No datasets found for query: '{query}'"
        
    # 3. Reciprocal Rank Fusion (RRF)
    rrf_scores = {}
    
    # Semantic Rank Fusion
    for rank, ds in enumerate(semantic_results, start=1):
        ds_id = ds["id"]
        rrf_scores[ds_id] = rrf_scores.get(ds_id, 0.0) + (1.0 / (60.0 + rank))
        
    # Keyword Rank Fusion
    for rank, ds in enumerate(keyword_results, start=1):
        ds_id = ds.get("id")
        if ds_id:
            rrf_scores[ds_id] = rrf_scores.get(ds_id, 0.0) + (1.0 / (60.0 + rank))
            
    # Sort IDs by RRF score descending
    sorted_ids = sorted(rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True)
    
    # Map of IDs to distances from semantic search for display
    semantic_map = {ds["id"]: ds for ds in semantic_results}
    
    # 4. Batch retrieve full records from DuckDB to filter for queryable/tabular datasets
    all_details = get_by_ids(sorted_ids)
    
    fused_results = []
    for ds_id in sorted_ids:
        if ds_id in all_details:
            ds = all_details[ds_id]
            ds["rrf_score"] = rrf_scores[ds_id]
            ds["distance"] = semantic_map[ds_id]["distance"] if ds_id in semantic_map else None
            
            # Check if this hit came from keyword, semantic, or both
            is_semantic = ds_id in semantic_map
            is_keyword = any(k.get("id") == ds_id for k in keyword_results)
            if is_semantic and is_keyword:
                ds["match_type"] = "Hybrid"
            elif is_semantic:
                ds["match_type"] = "Semantic"
            else:
                ds["match_type"] = "Keyword"
            fused_results.append(ds)
            
    # Crop to the requested limit
    fused_results = fused_results[:limit]
    
    if not fused_results:
        return f"No queryable tabular datasets found matching: '{query}'"
        
    md = [f"### Hybrid Search Results (RRF) for '{query}' (showing top {len(fused_results)})\n"]
    for ds in fused_results:
        title = ds["title"]
        org = ds["org"] or "Unknown Publisher"
        desc = _truncate_desc(ds["notes"])
        ds_id = ds["id"]
        
        # Resources summary
        resources = ds["resources"]
        csv_count = sum(1 for r in resources if r["format"] == "CSV")
        xlsx_count = sum(1 for r in resources if r["format"] in ("XLSX", "XLS"))
        parquet_count = sum(1 for r in resources if r["format"] == "PARQUET")
        json_count = sum(1 for r in resources if r["format"] == "JSON")
        datastore_count = sum(1 for r in resources if r["datastore_active"])
        
        res_summary_parts = []
        if csv_count:
            res_summary_parts.append(f"{csv_count} CSV")
        if xlsx_count:
            res_summary_parts.append(f"{xlsx_count} Excel")
        if parquet_count:
            res_summary_parts.append(f"{parquet_count} Parquet")
        if json_count:
            res_summary_parts.append(f"{json_count} JSON")
            
        res_summary = ", ".join(res_summary_parts) if res_summary_parts else "No tabular files"
        if datastore_count:
            res_summary += f" ({datastore_count} API-enabled datastores)"
            
        page = f"https://open.canada.ca/data/en/dataset/{ds_id}"
        
        md.append(f"**[{title}]({page})**")
        md.append(f"- **Publisher**: {org}")
        md.append(f"- **Description**: {desc}")
        md.append(f"- **Resources**: {res_summary}")
        md.append(f"- **Dataset id**: `{ds_id}` → `get_dataset('{ds_id}')`")
        
        match_info = f"Match: {ds['match_type']} (RRF: {ds['rrf_score']:.4f})"
        if ds["distance"] is not None:
            match_info += f" | Cosine Distance: {ds['distance']:.4f}"
        md.append(f"- **Search Info**: {match_info}")
        md.append("")
        
    return "\n".join(md)


@mcp.tool()
@log_telemetry("search_datasets")
def search_datasets(query: str, limit: int = 5) -> str:
    """
    Search the Government of Canada Open Data portal (open.canada.ca) for datasets.
    Uses the CKAN package_search API.

    Args:
        query: Keywords (e.g., "alberta well licences", "population census").
        limit: Max datasets to return (default 5).

    Returns:
        Markdown list of datasets with title, organization, description, and the
        dataset id to pass to get_dataset.
    """
    try:
        result = _ckan_get("package_search", q=query, rows=max(1, min(limit, 25)))
    except Exception as e:
        return f"Error searching open.canada.ca: {e}"

    results = result.get("results", [])
    if not results:
        return f"No datasets found for '{query}'."

    md = [f"### open.canada.ca results for '{query}' "
          f"({result.get('count', 0)} total, showing {len(results)})\n"]
    for ds in results:
        title = ds.get("title") or ds.get("name", "Untitled")
        org = (ds.get("organization") or {}).get("title", "Unknown org")
        desc = _truncate_desc(ds.get("notes", ""))
        ds_id = ds.get("id", "")
        n_res = len(ds.get("resources", []))
        page = f"https://open.canada.ca/data/en/dataset/{ds_id}"
        md.append(f"**[{title}]({page})**")
        md.append(f"- **Publisher**: {org}")
        md.append(f"- **Description**: {desc}")
        md.append(f"- **Resources**: {n_res}")
        md.append(f"- **Dataset id**: `{ds_id}` → `get_dataset('{ds_id}')`")
        md.append("")
    return "\n".join(md)


@mcp.tool()
@log_telemetry("get_dataset")
def get_dataset(dataset_id: str) -> str:
    """
    Get a dataset's metadata and all its resources (CKAN package_show).
    Accepts a dataset id/slug or a full open.canada.ca dataset URL.

    For each resource it reports the format and whether it is datastore-backed:
      - datastore_active: true  → query it server-side with query_datastore(resource_id, ...)
      - datastore_active: false → query the file with query_remote_file(url, sql)

    Returns:
        Markdown: title, org, description, and a resource table with ids and formats.
    """
    ds_id = _extract_dataset_id(dataset_id)
    try:
        ds = _ckan_get("package_show", id=ds_id)
    except Exception as e:
        return f"Error fetching dataset '{ds_id}': {e}"

    title = ds.get("title") or ds.get("name", "Untitled")
    org = (ds.get("organization") or {}).get("title", "Unknown org")
    desc = _truncate_desc(ds.get("notes", ""))
    page = f"https://open.canada.ca/data/en/dataset/{ds_id}"

    md = [f"## [{title}]({page})", f"**Publisher**: {org}",
          f"**Source**: {page}", f"**Description**: {desc}", ""]
    resources = ds.get("resources", [])
    md.append(f"### Resources ({len(resources)})\n")

    rows = []
    for r in resources:
        rows.append({
            "name": (r.get("name") or "—")[:60],
            "format": (r.get("format") or "?").upper(),
            "datastore": "✅ yes" if r.get("datastore_active") else "no",
            "resource_id": r.get("id", ""),
            "url": r.get("url", ""),
        })
    if rows:
        md.append(pd.DataFrame(rows).to_markdown(index=False))

    queryable = [r for r in rows if r["datastore"].startswith("✅")]
    md.append("")
    if queryable:
        rid = queryable[0]["resource_id"]
        md.append(f"**Tip**: `{queryable[0]['name']}` is datastore-backed → "
                  f"`query_datastore('{rid}', q='...')` (server-side, no download).")
    else:
        md.append("**Tip**: no datastore-backed resources here — use "
                  "`query_remote_file(url, sql)` on a CSV/Parquet resource above.")
    return "\n".join(md)


# =====================================================================
# SERVER-SIDE QUERY (datastore)  — the fast path, no download
# =====================================================================
@mcp.tool()
@log_telemetry("get_resource_fields")
def get_resource_fields(resource_id: str) -> str:
    """
    Get the column names and types for a datastore-backed resource — no data download.
    Call this before query_datastore to learn what columns/filters are available.

    Args:
        resource_id: The resource UUID (from get_dataset).

    Returns:
        Markdown table of field id + type, plus the resource's total row count.
    """
    try:
        result = _ckan_get("datastore_search", resource_id=resource_id, limit=0)
    except Exception as e:
        return (f"Error reading fields for '{resource_id}': {e}\n"
                f"(This resource may not be datastore-backed; try query_remote_file.)")
    fields = [f for f in result.get("fields", []) if f.get("id") != "_id"]
    if not fields:
        return "No fields returned (resource may not be datastore-backed)."
    df = pd.DataFrame([{"field": f["id"], "type": f.get("type", "?")} for f in fields])
    return (f"### Fields for `{resource_id}`  (total rows: {result.get('total', '?')})\n\n"
            + df.to_markdown(index=False))


@mcp.tool()
@log_telemetry("query_datastore")
def query_datastore(resource_id: str, q: str = "", filters: str = "",
                    sort: str = "", limit: int = 50, offset: int = 0) -> str:
    """
    Query a datastore-backed resource SERVER-SIDE via CKAN datastore_search.
    No file download — the portal's database does the filtering. Use this for large
    resources (the fast path for "find these rows in a huge table").

    CRITICAL: You must cite the dataset source URL in your response to the user.
    The source URL is included at the end of the tool's return text.

    Args:
        resource_id: Resource UUID (from get_dataset).
        q: Full-text search across all columns (e.g., "Calgary").
        filters: JSON object of exact-match filters, e.g. '{"province": "Alberta"}'.
        sort: Sort spec, e.g. "year desc" or "name asc".
        limit: Max rows to return (default 50, capped at 100).
        offset: Row offset for pagination (default 0).

    Returns:
        Markdown table of matching rows with a pagination hint.
    """
    params: Dict[str, Any] = {
        "resource_id": resource_id,
        "limit": max(1, min(limit, MAX_RESULT_ROWS)),
        "offset": max(0, offset),
    }
    if q:
        params["q"] = q
    if sort:
        params["sort"] = sort
    if filters:
        try:
            json.loads(filters)  # validate
            params["filters"] = filters
        except json.JSONDecodeError:
            return f"Error: `filters` must be valid JSON, e.g. '{{\"province\": \"Alberta\"}}'. Got: {filters}"

    try:
        result = _ckan_get("datastore_search", **params)
    except Exception as e:
        return (f"Error querying datastore '{resource_id}': {e}\n"
                f"(If this resource isn't datastore-backed, use query_remote_file.)")

    records = result.get("records", [])
    if not records:
        return f"Query ran, but no rows matched (total in resource: {result.get('total', '?')})."
    df = _clean_df(pd.DataFrame(records))

    # Resolve the parent dataset URL for citation
    citation = ""
    try:
        res_info = _ckan_get("resource_show", id=resource_id)
        pkg_id = res_info.get("package_id", "")
        if pkg_id:
            citation = (f"\n\n---\n**Source**: "
                        f"[open.canada.ca/data/en/dataset/{pkg_id}]"
                        f"(https://open.canada.ca/data/en/dataset/{pkg_id})")
    except Exception:
        pass

    return (f"### {result.get('total', len(records))} rows matched "
            f"(showing {len(records)} from offset {offset})\n\n"
            + _df_to_md(df, cap=limit, offset=offset, total=result.get("total"))
            + citation)


# =====================================================================
# FILE FALLBACK (DuckDB)  — for resources without a datastore
# =====================================================================
@mcp.tool()
@log_telemetry("preview_remote_file")
def preview_remote_file(file_url: str, max_rows: int = MAX_PREVIEW_ROWS,
                        sheet_name: str = "") -> str:
    """
    Preview the first rows of a remote CSV/JSON/Parquet/XLSX resource via DuckDB.
    Use for resources where datastore_active is false (plain file downloads).

    CRITICAL: You must cite the dataset source URL in your response to the user.
    The source URL is included at the end of the tool's return text.

    Args:
        file_url: Direct resource download URL (from get_dataset).
        max_rows: Rows to preview (default 15).
        sheet_name: For Excel only — which sheet to preview (default: first sheet).
                    Call list_excel_sheets first to see the available sheets.
    """
    if not file_url:
        return "Error: No file URL provided."
    low = file_url.lower()
    citation = f"\n\n---\n**Source**: [{file_url}]({file_url})"
    try:
        if low.endswith((".xlsx", ".xls")):
            df = _read_tabular(file_url, nrows=max_rows,
                               sheet_name=sheet_name or 0)
            label = f" (sheet: {sheet_name})" if sheet_name else ""
            return f"### Preview{label}\n\n" + _df_to_md(_clean_df(df), cap=max_rows) + citation
        if low.endswith((".zip", ".json")):
            df = _read_tabular(file_url, nrows=max_rows)
        else:
            df = run_query_with_timeout(
                f"SELECT * FROM '{file_url}' LIMIT {int(max_rows)}"
            )
        return "### Preview\n\n" + _df_to_md(_clean_df(df), cap=max_rows) + citation
    except Exception as e:
        return f"Error previewing file: {e}"


@mcp.tool()
@log_telemetry("get_file_schema")
def get_file_schema(file_url: str, sheet_name: str = "") -> str:
    """
    Get column names and types for a remote CSV/Parquet/JSON file via DuckDB DESCRIBE
    (minimal download). Use for non-datastore resources before query_remote_file.

    Args:
        file_url: Direct resource download URL.
        sheet_name: For Excel only — which sheet's schema to read (default: first).
    """
    if not file_url:
        return "Error: No file URL provided."
    low = file_url.lower()
    # Excel and ZIP must be read via pandas — DuckDB can't DESCRIBE them remotely.
    if low.endswith((".xlsx", ".xls")) or low.endswith(".zip"):
        try:
            df = _read_tabular(file_url, nrows=5, sheet_name=sheet_name or 0)
            schema = pd.DataFrame(
                [{"column_name": c, "column_type": str(df[c].dtype)} for c in df.columns]
            )
            label = f" (sheet: {sheet_name})" if sheet_name else ""
            suffix = " (extracted from ZIP)" if low.endswith(".zip") else label
            return f"### Schema (sampled){suffix}\n\n" + schema.to_markdown(index=False)
        except Exception as e:
            return f"Error reading file schema: {e}"
    try:
        df = run_query_with_timeout(f"DESCRIBE SELECT * FROM '{file_url}'")
        return "### Schema\n\n" + df[["column_name", "column_type"]].to_markdown(index=False)
    except Exception as e:
        # Fallback for formats DuckDB can't DESCRIBE remotely (json)
        try:
            df = _read_tabular(file_url, nrows=5)
            schema = pd.DataFrame(
                [{"column_name": c, "column_type": str(df[c].dtype)} for c in df.columns]
            )
            return "### Schema (sampled)\n\n" + schema.to_markdown(index=False)
        except Exception as e2:
            return f"Error reading schema: {e} / {e2}"


@mcp.tool()
@log_telemetry("list_excel_sheets")
def list_excel_sheets(file_url: str) -> str:
    """
    List every sheet in a remote Excel workbook with its row/column counts and columns.
    Many open.canada.ca Excel resources are multi-sheet and are NOT datastore-backed,
    so use this to see what's inside before previewing or querying a specific sheet.

    Args:
        file_url: Direct URL to an .xlsx/.xls resource (from get_dataset).
    """
    if not file_url:
        return "Error: No file URL provided."
    if not file_url.lower().endswith((".xlsx", ".xls")):
        return "Error: not an Excel file. Use get_file_schema for CSV/Parquet/JSON."
    try:
        xl = _excel_file(file_url)
    except Exception as e:
        return f"Error opening workbook: {e}"

    rows = []
    for name in xl.sheet_names:
        try:
            hdr = _detect_header_row(xl, name)
            df = xl.parse(name, header=hdr, nrows=200)
            df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed: \d+$")]
            cols = ", ".join(map(str, df.columns[:12]))
            if len(df.columns) > 12:
                cols += ", …"
            hdr_note = f" (header row {hdr})" if hdr > 0 else ""
            rows.append({"sheet": name, "columns": len(df.columns),
                         "rows(sampled)": len(df),
                         "header_row": hdr,
                         "column names": cols[:160] + hdr_note})
        except Exception as e:
            rows.append({"sheet": name, "columns": "?", "rows(sampled)": "?",
                         "header_row": "?", "column names": f"(error: {e})"})
    md = (f"### Workbook sheets ({len(xl.sheet_names)})\n\n"
          + pd.DataFrame(rows).to_markdown(index=False))
    md += ("\n\n_Next: `preview_remote_file(url, sheet_name='<sheet>')` or "
           "`query_excel_sheet(url, '<sheet>', sql)`._")
    return md


@mcp.tool()
@log_telemetry("query_excel_sheet")
def query_excel_sheet(file_url: str, sheet_name: str, sql_query: str) -> str:
    """
    Run a read-only DuckDB SQL query against a single sheet of a remote Excel workbook.
    Use '{sheet}' as the table placeholder. Call list_excel_sheets first to get names.

    CRITICAL: You must cite the dataset source URL in your response to the user.
    The source URL is included at the end of the tool's return text.

    Example:
        query_excel_sheet(url, "2024 Data",
            "SELECT region, SUM(amount) FROM '{sheet}' GROUP BY region")

    Args:
        file_url: Direct URL to an .xlsx/.xls resource.
        sheet_name: Exact sheet name (from list_excel_sheets).
        sql_query: SELECT query using '{sheet}' as the table reference.
    """
    if not (file_url and sheet_name and sql_query):
        return "Error: file_url, sheet_name and sql_query are all required."
    if _WRITE_RE.search(sql_query):
        return "Error: only read-only SELECT queries are permitted."
    try:
        df = _read_tabular(file_url, sheet_name=sheet_name)  # noqa: F841 (used by DuckDB)
    except Exception as e:
        return f"Error reading sheet '{sheet_name}': {e}"

    sql = sql_query.replace("'{sheet}'", "df").replace("{sheet}", "df")
    try:
        out = _query_dataframe(sql, df)
    except Exception as e:
        return f"Error executing query: {e}"
    if out.empty:
        return "Query ran successfully but returned 0 rows."
    citation = f"\n\n---\n**Source**: [{file_url}]({file_url}) — sheet: `{sheet_name}`"
    return _df_to_md(_clean_df(out)) + citation


@mcp.tool()
@log_telemetry("query_remote_file")
def query_remote_file(file_url: str, sql_query: str) -> str:
    """
    Run a read-only DuckDB SQL query directly on a remote file (CSV/Parquet/JSON/ZIP).
    Use '{file}' as the table placeholder. For datastore-backed resources prefer
    query_datastore (faster, server-side). ZIP files containing CSV are fully supported.

    CRITICAL: You must cite the dataset source URL in your response to the user.
    The source URL is included at the end of the tool's return text.

    Example:
        query_remote_file(url, "SELECT province, COUNT(*) FROM '{file}' GROUP BY province")

    Args:
        file_url: Direct resource download URL (CSV, Parquet, JSON, or ZIP of CSV).
        sql_query: SELECT query using '{file}' as the table reference.
    """
    if not file_url or not sql_query:
        return "Error: both file_url and sql_query are required."

    if _WRITE_RE.search(sql_query):
        return "Error: only read-only SELECT queries are permitted."

    citation = f"\n\n---\n**Source**: [{file_url}]({file_url})"
    low = file_url.lower()

    # ZIP files must be downloaded and extracted first — DuckDB httpfs can't unzip
    if low.endswith(".zip"):
        try:
            df = _read_tabular(file_url)
        except Exception as e:
            return f"Error reading ZIP file: {e}"
        sql = sql_query.replace("'{file}'", "df").replace("{file}", "df")
        try:
            out = _query_dataframe(sql, df)
        except Exception as e:
            return f"Error executing query on ZIP contents: {e}"
        if out.empty:
            return "Query ran successfully but returned 0 rows."
        return _df_to_md(_clean_df(out)) + citation

    # Standard DuckDB httpfs path for CSV / Parquet / JSON
    sql = sql_query.replace("'{file}'", f"'{file_url}'").replace("{file}", f"'{file_url}'")
    try:
        df = run_query_with_timeout(sql)
    except TimeoutError as e:
        return str(e)
    except Exception as e:
        # DuckDB httpfs uses its own HTTP stack (no User-Agent, no TLS 1.2
        # fallback) and gets reset by some government hosts that _robust_get
        # handles fine. Download the bytes ourselves and query the buffer.
        try:
            df_local = _read_tabular(file_url)
            local_sql = sql_query.replace("'{file}'", "df").replace("{file}", "df")
            df = _query_dataframe(local_sql, df_local)
        except Exception as e2:
            return f"Error executing query: {e}\n(Local download fallback also failed: {e2})"
    if df.empty:
        return "Query ran successfully but returned 0 rows."
    return _df_to_md(_clean_df(df)) + citation


@mcp.tool()
@log_telemetry("read_pdf")
def read_pdf(file_url: str, pages: str = "1-10") -> str:
    """
    Extract text from a remote PDF resource (reports, publications, documentation).
    Many open.canada.ca datasets are PDF-only — use this to read them after discovery.

    CRITICAL: You must cite the dataset source URL in your response to the user.
    The source URL is included at the end of the tool's return text.

    Args:
        file_url: Direct URL to a .pdf resource (from get_dataset).
        pages: Page range to extract, e.g. "1-10", "5", or "3-7" (1-indexed,
               default first 10 pages). Keep ranges small — pages can be long.
    """
    if not file_url:
        return "Error: No file URL provided."
    m = re.fullmatch(r"\s*(\d+)(?:\s*-\s*(\d+))?\s*", pages or "1-10")
    if not m:
        return "Error: pages must look like '1-10' or '5'."
    start = int(m.group(1))
    end = int(m.group(2) or m.group(1))
    if start < 1 or end < start or end - start + 1 > 20:
        return "Error: invalid range (max 20 pages per call, 1-indexed)."

    try:
        resp = _robust_get(file_url, timeout=HTTP_TIMEOUT * 3)
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(resp.content))
    except Exception as e:
        return f"Error opening PDF: {e}"

    n = len(reader.pages)
    if start > n:
        return f"Error: PDF has only {n} pages."
    end = min(end, n)

    parts = [f"### PDF text — pages {start}–{end} of {n}\n"]
    for i in range(start - 1, end):
        try:
            text = (reader.pages[i].extract_text() or "").strip()
        except Exception as e:
            text = f"(extraction failed: {e})"
        parts.append(f"--- page {i + 1} ---\n{text if text else '(no extractable text — possibly scanned image)'}")
    if end < n:
        parts.append(f"\n_{n - end} more pages — call again with pages='{end + 1}-{min(end + 10, n)}'._")
    parts.append(f"\n---\n**Source**: [{file_url}]({file_url})")
    return "\n\n".join(parts)


# =====================================================================
# PROMPTS  — workflow shortcuts surfaced in Claude Desktop's prompt menu
# =====================================================================
@mcp.prompt()
def query_canada_data(question: str) -> str:
    """
    Full pipeline: natural-language question → find datasets → query → cite source.
    Use this as your starting point for any Canada open data question.
    """
    return (
        f"Answer this question using Government of Canada open data: {question!r}\n\n"
        "Follow this workflow exactly:\n"
        "1. Call `semantic_search_datasets(question)` to find the most relevant datasets.\n"
        "2. Pick the best match and call `get_dataset(id)` to see its resources.\n"
        "3. For each resource, choose the right query path:\n"
        "   - datastore_active = true  → `get_resource_fields` then `query_datastore`\n"
        "   - false + CSV/Parquet      → `get_file_schema` then `query_remote_file`\n"
        "   - false + Excel            → `list_excel_sheets` then `query_excel_sheet`\n"
        "4. In your final answer, integrate numbered inline citations (e.g. [1], [2]) next to any data points, statistics, or facts you mention.\n"
        "5. At the very end of your response, add a '### 📚 Sources' section containing a clean markdown table formatting the sources like this:\n"
        "   | Citation | Source Dataset / Resource | Publisher | Link |\n"
        "   |---|---|---|---|\n"
        "   | [1] | [Dataset Title](Dataset Page URL) | Organization Name | [Direct File Link / API](Direct URL) |"
    )


@mcp.prompt()
def explore_dataset(topic: str) -> str:
    """
    Discover and preview datasets on a topic without writing any queries.
    Good for 'what data exists on X?' questions.
    """
    return (
        f"I want to explore what Government of Canada open data exists on: {topic!r}\n\n"
        "Please:\n"
        "1. Call `semantic_search_datasets(topic)` — show me the top results.\n"
        "2. For the most relevant dataset, call `get_dataset(id)` to list its resources.\n"
        "3. Pick the most interesting resource and call `preview_remote_file(url)` "
        "   (or `list_excel_sheets` if it's an Excel file) to show a sample of the data.\n"
        "4. Summarise what columns/fields are available and what questions this data could answer.\n"
        "5. Format your output with inline citations (e.g. [1]) and end with a '### 📚 Sources' table:\n"
        "   | Citation | Dataset | Publisher | Link |\n"
        "   |---|---|---|---|\n"
        "   | [1] | [Dataset Title](Dataset Page URL) | Organization | [Direct File Link](Direct URL) |"
    )


@mcp.prompt()
def compare_datasets(topic: str) -> str:
    """
    Find multiple datasets on a topic and compare their coverage, recency, and queryability.
    """
    return (
        f"Find and compare Government of Canada open datasets related to: {topic!r}\n\n"
        "Steps:\n"
        "1. Call `semantic_search_datasets(topic, limit=6)` to get a broad set of results.\n"
        "2. Compare the datasets using a markdown table. Show title, publisher, format, queryability, and last modified date.\n"
        "3. Recommend which dataset is best for analytical queries and why.\n"
        "4. Integrate inline citations (e.g. [1]) for any facts/dates you mention, and include a '### 📚 Sources' section at the very end with clickable links to the datasets' portal pages formatted as a clean markdown table:\n"
        "   | Citation | Dataset | Publisher | Link |\n"
        "   |---|---|---|---|\n"
        "   | [1] | [Dataset Title](Dataset Page URL) | Organization | [Direct File Link](Direct URL) |"
    )


if __name__ == "__main__":
    mcp.run()
