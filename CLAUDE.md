# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run build        # compile TypeScript to dist/
npx tsc --noEmit    # type-check without emitting (use this to verify changes)

npm run setup        # Phase 1: create Oracle tables + scheduler job
npm run report       # Phase 2: read samples, generate report + .env
npm run teardown     # Phase 3: drop tables and job
```

The `report` command accepts `--archive-disk-gb <n>` (default 250) for the total archive filesystem size.

There are no tests. `npx tsc --noEmit` is the only automated correctness check.

## Architecture

The tool has three phases, each in its own source file, driven by `src/index.ts` (Commander CLI):

**`src/db.ts`** — thin Oracle connection layer (`withConnection`, `execute`, `queryRows`). All Oracle I/O goes through here. `oracledb.outFormat` is set to `OUT_FORMAT_OBJECT` globally.

**`src/setup.ts`** — Phase 1. Creates two tables in the Debezium user's schema (`DBZ_DIAG_SAMPLES`, `DBZ_DIAG_STATIC`) and an Oracle Scheduler job (`DBZ_DIAG_SAMPLER`). The job body is a PL/SQL block embedded as a JavaScript template string and passed to `DBMS_SCHEDULER.CREATE_JOB` as a `PLSQL_BLOCK`. The job runs every `SAMPLE_INTERVAL_MINUTES` (default 15) and inserts 8 time-series metrics into `DBZ_DIAG_SAMPLES`. One-time static data (redo log config, LOB columns, supplemental logging status, etc.) is collected immediately and stored in `DBZ_DIAG_STATIC`.

**`src/report.ts`** — Phase 2. Reads all rows from both tables, computes `min/avg/p95/max` per metric using `PERCENTILE_CONT` in Oracle, then runs `computeRecommendations()` which produces a `Recommendations` object. Outputs two files to `cwd`: `dbz-diag-report.md` (human-readable) and `dbz-recommended.env` (ready-to-use Debezium env vars).

**`src/teardown.ts`** — Phase 3. Drops both tables and the scheduler job.

## Key design decisions

- **All sampling runs inside Oracle** via the scheduler job — no long-running process is needed between setup and report.
- **Metrics are per-interval counts/sums**; the report multiplies by `60 / SAMPLE_INTERVAL_MINUTES` to convert to per-hour rates.
- **`archive_window_hours`** (span of non-deleted archive logs on disk) is the direct ORA-00308 risk signal. The report compares it against the computed `archiveRetentionHours` and warns if current cleanup policy is already too aggressive.
- **`log.mining.transaction.retention.ms`** is set to `p95_oldest_txn × 2` (uncapped) — this must cover the full observed transaction lifetime so the LogMiner watermark can advance without abandoning live transactions.
- The recommendation engine lives entirely in `computeRecommendations()` (`report.ts:197`). Retention formula, LOB logic, batch sizing, and all warnings are computed there.

## Environment variables

Configured via `.env` (see `.env.example`):

| Variable | Purpose |
|---|---|
| `ORACLE_HOST/PORT/SERVICE/USER/PASSWORD` | Database connection |
| `CAPTURE_SCHEMA` | Schema Debezium will capture (required for setup) |
| `CAPTURE_TABLE_PATTERN` | Oracle `REGEXP_LIKE` pattern for captured tables (required for setup) |
| `SAMPLE_INTERVAL_MINUTES` | Sampling frequency, default 15 |

`ARCHIVE_DISK_TOTAL_GB` in `.env.example` is informational only — the code reads it from the `--archive-disk-gb` CLI flag, not from env.

## Reference

See `ANALYSIS.md` for the Oracle and Debezium documentation sources that back each calculation (redo log sizing, archive retention formula, connector parameters, ORA-00308 root cause).
