# Roadmap (local notes — not published)

## Done

- [x] P1 — Government-host download reliability
      Browser-ish User-Agent on all requests, broadened transient-error
      retry set (ConnectionError, SSLError, ChunkedEncodingError), TLS 1.2
      fallback session, and a local-download fallback for the non-ZIP
      `query_remote_file` path when DuckDB httpfs itself gets reset
      (httpfs has no UA / no TLS pin of its own).

## P2a — Curated non-CKAN source catalog (next up)

The federal open.canada.ca CKAN catalog doesn't carry every authoritative
Canadian dataset. The wildfires investigation proved this concretely: the
real "fires by province by year" table lives in NRCan's National Forestry
Database (nfdp.ccfm.org / cwfis.cfs.nrcan.gc.ca), not on the portal.

Plan:
- Hand-curate a small set of high-value non-CKAN sources (National Forestry
  Database, StatCan Web Data Service tables not mirrored to CKAN, CIFFC).
- Write their metadata (title, notes, keywords, direct data URL(s)) as
  synthetic records in the same shape `build_index.py` produces, embed them
  with the same bge-small model, and upsert into `catalog.duckdb` alongside
  the portal records.
- `semantic_search_datasets` then surfaces them automatically via the
  existing RRF fusion — no new tool needed, just a richer corpus.
- Keep the list small and reviewed by hand (accuracy > coverage) — this is
  curation, not a second harvester.

## P2b — `query_socrata` connector

Several provinces/cities (Ontario, Alberta, Calgary, and most municipal
open-data portals) run Socrata, which supports server-side SoQL
aggregation (`$select`, `$where`, `$group`, `$order`) — the same
"filter/aggregate server-side, don't download" philosophy as
`query_datastore` for CKAN.

Plan:
- `query_socrata(domain, dataset_id, select="", where="", group="",
  order="", limit=50)` — builds a SoQL query against
  `https://{domain}/resource/{dataset_id}.json`.
- Small hardcoded registry of known Socrata domains (data.ontario.ca,
  data.calgary.ca, open.alberta.ca, etc.) so discovery can suggest it.
- Bigger scope than P2a: this is a new query surface with its own
  discovery problem (which datasets exist on which domain?) — treat as a
  v1.1 feature, not a patch. Needs its own design pass before starting.
