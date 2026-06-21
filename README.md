# SyncLite Validator – End-to-End Integration Testing Tool

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) – Build Anything, Sync Anywhere.

## What is SyncLite Validator?

**SyncLite Validator** is the end-to-end (E2E) integration testing and data quality verification tool for SyncLite pipelines. It drives synthetic workloads through the full SyncLite pipeline – from edge device through staging storage through consolidation into the destination – and automatically validates that every row, every transaction, and every schema change arrived correctly and in the expected state.

It is used by SyncLite developers for regression testing of the platform and by adopters to verify their pipeline configuration before going to production.

```
Validator (workload generator)
       |  SQL operations via SyncLite Logger / SyncLite DB
       ?
  Edge Device  -->  Staging Storage  -->  SyncLite Consolidator  -->  Destination DB
       |                                                                      |
       +------------------- Validator (data comparison) <---------------------+
```

## Key Features

- **Automated E2E verification** – generates a configurable workload, waits for consolidation, then compares source and destination row by row
- **Multiple device types** – validates all SyncLite device types (SQLite, DuckDB, Derby, H2, HyperSQL, Streaming)
- **Schema evolution testing** – validates DDL changes (ALTER TABLE, new tables) propagate correctly
- **Transaction integrity** – verifies committed vs. rolled-back transactions are reflected correctly at the destination
- **Configurable workloads** – control table count, row count, update/delete ratios, and concurrency
- **Detailed diff reports** – row-level mismatch reports with source vs. destination values
- **Web UI** – configure test runs, view progress, and inspect results from a browser

## Quick Start

1. Deploy the SyncLite platform and start a Consolidator job (see [platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)).
2. Open http://localhost:8080/synclite-validator
3. Configure the validator: point it at the SyncLite Logger config and the destination DB connection.
4. Click **Run Validation**. The validator will generate a workload, let consolidation catch up, then compare all data automatically.
5. Review the pass/fail report and any row-level diffs.

## Build

```bash
cd synclite-validator/root
mvn -Drevision=1.0.0 clean install
```

Built WAR: `root/web/target/synclite-validator-oss.war`

## Related Components

| Component | Role |
|---|---|
| [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) | The pipeline under test |
| [SyncLite Logger](https://github.com/syncliteio/synclite-logger-java) | Used by the validator to generate the edge-side workload |
| [SyncLite Job Monitor](https://github.com/syncliteio/synclite-job-monitor) | Can schedule and track validation runs |

## Documentation & Community

- Full documentation: https://github.com/syncliteio/SyncLite/blob/main/DOCUMENTATION.md
- Website: https://www.synclite.io
- Community: https://github.com/syncliteio/SyncLite/issues

---

? Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)

