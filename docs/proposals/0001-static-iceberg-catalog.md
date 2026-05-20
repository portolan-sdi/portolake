# Portolan as a Static Iceberg REST Catalog

**Proposal:** Allow publishing a Portolan as a static Iceberg REST Catalog hosted on object storage — no server, queryable directly from DuckDB, and federable with other Portolans in the same analytical session.

---

## Context

Portolan already distributes its contents as static files: STAC for the catalog, COG for raster, GeoParquet for vector. The entire cloud-native stack we've adopted fits the same pattern: upload files to object storage, no services to maintain.

What's missing is a **queryable index** that conforms to a standard modern analytical tools speak natively. The good news: **the Iceberg REST spec can be materialized as a tree of static JSON files**. That closes the loop — Portolan publishes Iceberg-REST-compliant catalogs without needing to run Polaris, Nessie, or any other service.

## How it works

The generator (already implemented on the `add-sdi-experiment` branch of the original portolan repo) emits a `v1/` tree that respects the Iceberg REST spec:

```
v1/
├── config
└── catalog/namespaces/
    ├── ne_admin_0/
    │   └── tables/countries     (metadata JSON pointing to existing Parquet)
    └── ne_admin_1/
        └── tables/provinces
```

You upload that tree to R2 (or S3, GCS, Azure — any backend that serves static files over HTTP). DuckDB connects directly to the endpoint:

```sql
INSTALL iceberg; LOAD iceberg;
ATTACH '' AS portolan (
  TYPE ICEBERG,
  ENDPOINT 'https://catalog.example.com',
  AUTHORIZATION_TYPE 'none'
);
SELECT * FROM portolan.ne_admin_0.countries LIMIT 10;
```

No server. No running process. No DB to back up. CDN-friendly by default. Hosting a complete public catalog costs on the order of **$5/month on R2**.

## Federation: multiple Portolans in one session

This is the part that most differentiates this approach. In the same DuckDB session you can attach several Portolans and run queries that cross catalogs:

```sql
ATTACH '' AS public_admin (
  TYPE ICEBERG,
  ENDPOINT 'https://admin.example.com'
);

ATTACH '' AS imagery_carto (
  TYPE ICEBERG,
  ENDPOINT 'https://imagery.carto.com',
  AUTHORIZATION_TYPE 'oauth2',
  TOKEN 'eyJ...'
);

SELECT i.scene_id, c.name_es
FROM imagery_carto.satellite.scenes i
JOIN public_admin.ne_admin_0.countries c
  ON ST_Intersects(i.geom, c.geom)
WHERE i.acquisition_date > '2025-01-01';
```

A join between a private dataset (imagery with auth) and a public one (administrative boundaries), resolved in a single analytical query. That's genuinely novel in the geospatial ecosystem.

## Per-catalog authentication

Each Portolan has its own identity. The client configures credentials per connection, the same way we already do with different S3 buckets.

Three possible modes:

- **Fully public** — no credentials, open reads (NaturalEarth, government open data)
- **Fully private** — credentials required for everything (internal corporate catalogs)
- **Hybrid** — a public subset by default; authenticating unlocks additional namespaces or tables

The initial prototype uses storage-layer auth (bucket policies / signed URLs). The hybrid mode requires a lightweight auth endpoint, but the bulk of the catalog remains static.

## Use cases

- Publishing public catalogs for a few dollars a month
- Commercial publishers monetizing access to premium datasets
- Organizations with data on their own object storage that want to expose it internally
- Federation across organizations (CARTO + HDX, for example) without centralizing anything

## How it fits portolake's current direction

[ADR-0003](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0003-plugin-architecture.md) already defines a plugin architecture. **Static-catalog and server-based lakehouse can coexist as two backends of the same plugin**, selectable via configuration:

| Mode | Best for |
|---|---|
| **Server-based lakehouse** (current) | Concurrent writes, strict ACID, transactional time travel |
| **Static catalog** | Publishing, reading, federation, cheap hosting |

They're not competitors — they solve different problems. The choice depends on the publisher's use case, not on a global architectural decision.

## Prototype status

Already implemented and working on the `add-sdi-experiment` branch of the original portolan repo (latest commit `d645577`):

- Complete generator for the Iceberg REST static tree (`iceberg_rest_catalog.py`)
- Iceberg metadata + manifest generation (`iceberg_metadata.py`)
- Sync to R2 via obstore (S3, GCS, Azure also supported) (`catalog_state.py`)
- End-to-end tested with DuckDB
- No outstanding TODOs/FIXMEs

Ready for a walkthrough and a decision on how to bring it into the current portolake.

## Questions to discuss

1. Should we add this mode to portolake as a second backend, or does it live better as a separate plugin?
2. Auth as a cross-cutting layer of the plugin architecture, or configured per backend?
3. What's CARTO's priority use case right now — inter-org federation, or pure public publishing?
4. If we move forward, which piece do we port first: the static generator, or the per-catalog auth model?
