# Debezium Oracle CDC Diagnostic Tool — Sources & Rationale

This document lists the official sources backing the calculations and recommendations produced by this tool.

---

## Redo log sizing (target: 3–5 switches/hour)

- [Oracle/Siebel Redo Log Sizing Guide](https://docs.oracle.com/cd/G26828_02/books/SiebInst/c-Guidelines-for-Sizing-Redo-Logs-for-an-Oracle-Database-aif1432252.html) — explicitly states no more than **5 switches/hour** during peak DML activity
- [Oracle TDPPT: Redo Log Size](https://docs.oracle.com/en/database/oracle/oracle-database/26/tdppt/redo-log-size.html) — general redo log sizing guidance
- [Oracle DBA Guide: Online Redo Logs](https://docs.oracle.com/html/E25494_01/onlineredo002.htm) — recommends one switch every 15–30 minutes (2–4/hour)

> **Note:** `V$INSTANCE_RECOVERY.OPTIMAL_LOGFILE_SIZE` (available when `FAST_START_MTTR_TARGET` is set) is Oracle's own workload-specific sizing signal and could supplement the sampled switch rate in a future version.

---

## Archive retention and ORA-00308

The tool computes `archiveRetentionHours` as:

```
max(
  p95 oldest transaction + 1h safety buffer,
  3 × switch interval + archive write time + 15 min overhead + 1h buffer,
  2h absolute floor
)
```

Sources:

- [Oracle LogMiner Utility](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html) — "all archive logs from all redo threads active during that range must be present"; retention must cover the full span from oldest open transaction's start SCN to the current position
- [ORA-00308 Error Reference](https://docs.oracle.com/error-help/db/ora-00308/) — raised when LogMiner tries to open an archive log that has been deleted or is inaccessible
- [Debezium: Oracle Does Not Contain SCN](https://debezium.io/blog/2025/07/16/oracle-does-not-contain-scn/) — operational walkthrough of when ORA-00308 fires in a CDC context and how to recover; confirms that archive logs purged faster than Debezium processes them is the primary cause

---

## Debezium connector parameters

Primary reference for all connector properties:
[Debezium Oracle Connector Reference](https://debezium.io/documentation/reference/stable/connectors/oracle.html)

| Parameter | Default | Rationale |
|---|---|---|
| `log.mining.transaction.retention.ms` | `0` (retain all) | Must be ≥ longest observed transaction and < archive retention, or the low-watermark SCN cannot advance and old archives stay pinned |
| `lob.enabled` | `false` | When `true`, the low-watermark SCN advances more conservatively — archive logs are pinned further back. Disabled by default to avoid unnecessary retention pressure |
| `log.mining.query.filter.mode` | `none` | `regex` reduces LogMiner overhead when capturing a small subset of schema tables; recommended when captured tables < 50% of schema |
| `heartbeat.interval.ms` | `0` (disabled) | Keeps the stored offset SCN advancing on idle streams; without it the offset can fall behind the archive window and trigger ORA-00308 on restart |
| `log.mining.batch.size.default` | `20000` | Starting SCN interval per LogMiner session; tool scales this with table capture ratio |
| `log.mining.batch.size.max` | `100000` | Ceiling for the adaptive SCN window |

Additional reading:
- [Debezium Oracle Series Part 1](https://debezium.io/blog/2022/09/30/debezium-oracle-series-part-1/) — supplemental logging requirements (`ALTER DATABASE ADD SUPPLEMENTAL LOG DATA` minimum; table-level `ALL COLUMNS` for full before-images)
- [Debezium Oracle Series Part 3](https://debezium.io/blog/2023/06/29/debezium-oracle-series-part-3/) — `transaction.retention.ms`, heartbeat strategy, and batch sizing adaptive behaviour in depth

---

## Oracle database parameters

- [`ARCHIVE_LAG_TARGET`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ARCHIVE_LAG_TARGET.html) — forces a log switch after N seconds regardless of redo volume; critical for low-traffic databases where natural switches may be hours apart, causing Debezium's offset SCN to stagnate and eventually fall outside the archive window
