---
hide:
  - navigation
  - toc
---

# Portolake

**Lakehouse-grade versioning for Portolan catalogs**

--8<-- "README.md:intro"

[Get Started](#installation){ .md-button .md-button--primary }
[View on GitHub](https://github.com/portolan-sdi/portolake){ .md-button }

---

## Why Portolake?

<div class="grid cards" markdown>

- :material-shield-check:{ .lg .middle } **ACID Transactions**

    ---

    Concurrent writes with full transactional guarantees via Apache Iceberg

- :material-history:{ .lg .middle } **Rollback**

    ---

    Roll back to any previous version — creates a new version preserving full history

- :material-delete-sweep:{ .lg .middle } **Prune**

    ---

    Clean up old snapshots while keeping the N most recent versions

- :material-database:{ .lg .middle } **Any Catalog Backend**

    ---

    SQLite, PostgreSQL, REST, AWS Glue, DynamoDB, Hive, or BigQuery

- :material-puzzle:{ .lg .middle } **Plugin Architecture**

    ---

    Drop-in replacement for Portolan's built-in backend — no code changes needed

- :material-tag-multiple:{ .lg .middle } **Semantic Versioning**

    ---

    Automatic version bumps: minor for non-breaking, major for breaking changes

</div>

---

--8<-- "README.md:features"

--8<-- "README.md:installation"

--8<-- "README.md:quickstart"

--8<-- "README.md:configuration"

## Roadmap

Portolake currently provides the **Apache Iceberg backend** for vector/tabular data (GeoParquet). Future work includes:

- **[Icechunk](https://icechunk.io/) backend** — Versioned storage for array/raster data (COG, NetCDF, HDF, Zarr via VirtualiZarr), as defined in [ADR-0015](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md)
- **Drift detection** — Full local/remote drift comparison (currently a stub)
- **Schema evolution** — Automated breaking change detection across versions

---

## Next Steps

<div class="grid cards" markdown>

- :material-cog:{ .lg .middle } **[Configuration](reference/configuration.md)**

    ---

    Configure catalog backends for production

- :material-api:{ .lg .middle } **[API Reference](reference/api.md)**

    ---

    Full backend API documentation

- :material-account-group:{ .lg .middle } **[Contributing](contributing.md)**

    ---

    Learn how to get involved in development

</div>

---

<small>
**License**: Apache 2.0 — [View on GitHub](https://github.com/portolan-sdi/portolake/blob/main/LICENSE)
</small>
