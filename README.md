# OpenMCP Canada — Discover and query 25,000+ Canadian government datasets

An MCP server that lets Claude (or any MCP client) discover and query the
Government of Canada Open Data portal ([open.canada.ca](https://open.canada.ca))
in plain English.

Ask *"how do interest rates affect housing prices?"* and Claude finds the right
StatCan tables, queries them, and answers with citations — no SQL knowledge, no
CSV downloads, no hunting through the portal.

**Zero API keys required.** Semantic search runs on a local embedding model
(bge-small-en-v1.5 via [fastembed](https://github.com/qdrant/fastembed)); querying
goes through the portal's public CKAN API and DuckDB.

## How it works

```
"which neighbourhoods in Toronto have the worst air quality?"
        │
        ▼
semantic_search_datasets ──── hybrid search: local vector index (24k datasets,
        │                     DuckDB + bge-small embeddings) fused with the
        │                     portal's keyword search via Reciprocal Rank Fusion
        ▼
get_dataset ────────────────── resources + which are API-queryable
        │
        ├─ datastore-backed? ──▶ query_datastore    (server-side, no download)
        ├─ CSV / Parquet?    ──▶ query_remote_file  (DuckDB streams over HTTP)
        ├─ ZIP of CSV?       ──▶ query_remote_file  (auto-extracted — StatCan bulk files)
        ├─ Excel?            ──▶ list_excel_sheets → query_excel_sheet
        └─ PDF report?       ──▶ read_pdf           (page-ranged text extraction)
```

Every response includes a source link back to open.canada.ca.

## Quick start

```bash
git clone https://github.com/<you>/openMCP.git
cd openMCP
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

### Get the search index

The semantic index (`catalog.duckdb`, ~120MB) is too large for git. Download it
from the [latest release](../../releases) and put it in the project root.

### Use with Claude Code

From the project directory:

```bash
claude mcp add openmcp -- ./venv/bin/python ./mcp_server.py
```

Or add a `.mcp.json` in your project root (checked into your own projects, so
teammates get it too):

```json
{
  "mcpServers": {
    "openmcp": {
      "command": "/absolute/path/to/openMCP/venv/bin/python",
      "args": ["/absolute/path/to/openMCP/mcp_server.py"]
    }
  }
}
```

Then just ask: *"Find data on boil water advisories and tell me which provinces
have the most."* Check it's connected with `/mcp`.

### Use with Claude Desktop

Add to `claude_desktop_config.json`
(macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "openmcp": {
      "command": "/absolute/path/to/openMCP/venv/bin/python",
      "args": ["/absolute/path/to/openMCP/mcp_server.py"]
    }
  }
}
```

Restart Claude Desktop; the tools appear under the 🔌 icon.

## Tools

| Tool | What it does |
|---|---|
| `semantic_search_datasets(query)` | Hybrid semantic + keyword dataset discovery (RRF) |
| `search_datasets(query)` | Plain keyword search (CKAN `package_search`) |
| `get_dataset(id)` | A dataset's resources + which are API-queryable |
| `get_resource_fields(resource_id)` | Columns/types of a datastore resource, no download |
| `query_datastore(resource_id, ...)` | **Server-side** filter/search — the fast path |
| `get_file_schema(url)` | Schema of a remote file (DuckDB `DESCRIBE`, minimal download) |
| `preview_remote_file(url)` | First rows of a remote CSV/Parquet/JSON/Excel/ZIP |
| `query_remote_file(url, sql)` | Read-only DuckDB SQL on a remote file (ZIP auto-extracted) |
| `list_excel_sheets(url)` | Sheets, shapes, and columns of a workbook (header auto-detected) |
| `query_excel_sheet(url, sheet, sql)` | SQL against one sheet of a workbook |
| `read_pdf(url, pages)` | Page-ranged text extraction from PDF resources |

Plus three MCP prompts (`query_canada_data`, `explore_dataset`,
`compare_datasets`) that encode the full workflow for one-click use.

## Design notes

- **Server-side first.** Resources with `datastore_active: true` are filtered by
  the portal's own database (`datastore_search`) — only matching rows travel.
  Files are the fallback, streamed by DuckDB over HTTP range requests where
  possible.
- **The vector "database" is one DuckDB file.** 24k × 384-dim vectors,
  brute-force cosine scan — single-digit milliseconds, no ANN index or vector
  service needed at this scale.
- **Real-world Excel/CSV handling**: multi-sheet workbooks, title rows before
  headers (auto-detected), bilingual descriptions, zipped StatCan bulk tables,
  multiple encodings.
- **Read-only by construction**: SQL is screened against write/DDL patterns;
  CKAN access is GET-only.
- **Refresh** the index without a full rebuild:
  `venv/bin/python semantic/build_index.py --refresh 1000` re-indexes the 1000
  most recently modified datasets.
- **Rebuilding from scratch** is optional — most users should just download the
  release asset. If you want to: `venv/bin/python semantic/build_index.py`
  (~15 min catalogue download + 10-40 min embedding on CPU). With
  `pip install torch sentence-transformers` it auto-detects Apple
  Silicon/CUDA and runs ~7x faster.

## Limitations

- Discovery covers datasets with tabular (CSV/Excel/Parquet/JSON) or
  PDF/TXT resources — ~24k of the portal's ~47k entries. Purely geospatial/HTML
  datasets are reachable via keyword search only.
- Some StatCan mirrors on the portal are terminated series; check date coverage
  (the current series usually exists under a near-identical title).
- `datastore_search_sql` is disabled on open.canada.ca, so server-side querying
  uses `q`/`filters`/`sort` rather than raw SQL.

## License

MIT
