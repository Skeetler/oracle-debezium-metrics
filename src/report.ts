import oracledb from "oracledb";
import { DbConfig, withConnection, queryRows } from "./db";
import * as fs from "fs";
import * as path from "path";

const SAMPLE_TABLE = "DBZ_DIAG_SAMPLES";
const STATIC_TABLE = "DBZ_DIAG_STATIC";

interface MetricStats {
  min: number;
  max: number;
  avg: number;
  p95: number;
  samples: number;
}

interface DiagReport {
  // Observed metrics
  switchesPerHour: MetricStats;
  archiveGbPerHour: MetricStats;
  avgArchiveFileSizeGb: number;
  oldestTxnMinutes: MetricStats;
  activeTxnCount: MetricStats;
  archiveWindowHours: MetricStats;
  archiveDiskUsedGb: MetricStats;
  samplingDurationHours: number;

  // Static config
  redoLogConfig: any[];
  lobColumns: any[];
  capturedTableCount: number;
  schemaTableCount: number;
  supplementalLogging: any;
  archiveLagTarget: number;
  maxStringSize: string;
  captureSchema: string;
  captureTablePattern: string;

  // Computed recommendations
  recommendations: Recommendations;
}

interface Recommendations {
  redoLogSizeGb: number;
  redoLogGroups: number;
  archiveRetentionHours: number;
  archiveRetentionDiskGb: number;
  lobEnabled: boolean;
  lobReason: string;
  transactionRetentionMs: number;
  heartbeatIntervalMs: number;
  batchSizeDefault: number;
  batchSizeMax: number;
  maxRetries: number;
  queryFilterMode: string;
  archiveLogOnlyMode: boolean;
  warnings: string[];
}

export async function report(cfg: DbConfig): Promise<void> {
  await withConnection(cfg, async (conn) => {
    console.log("Reading collected data...\n");

    const duration = await getSamplingDuration(conn, cfg.user);
    if (duration < 1) {
      console.error("✗ Less than 1 hour of data collected. Let the sampler run longer.");
      console.error(`  Current duration: ${duration.toFixed(1)} hours`);
      return;
    }

    const intervalMin = parseInt(process.env.SAMPLE_INTERVAL_MINUTES ?? "15", 10);
    const hourMultiplier = 60 / intervalMin;

    const diagReport: DiagReport = {
      switchesPerHour: await getMetricStats(conn, cfg.user, "switches", hourMultiplier),
      archiveGbPerHour: await getMetricStats(conn, cfg.user, "archive_gb", hourMultiplier),
      avgArchiveFileSizeGb: await getMetricAvg(conn, cfg.user, "avg_archive_size_gb"),
      oldestTxnMinutes: await getMetricStats(conn, cfg.user, "oldest_txn_mins"),
      activeTxnCount: await getMetricStats(conn, cfg.user, "active_txn_count"),
      archiveWindowHours: await getMetricStats(conn, cfg.user, "archive_window_hours"),
      archiveDiskUsedGb: await getMetricStats(conn, cfg.user, "archive_disk_used_gb"),
      samplingDurationHours: duration,

      redoLogConfig: await getStatic(conn, cfg.user, "redo_log_config"),
      lobColumns: await getStatic(conn, cfg.user, "lob_columns"),
      capturedTableCount: parseInt(await getStaticRaw(conn, cfg.user, "captured_table_count") ?? "0", 10),
      schemaTableCount: parseInt(await getStaticRaw(conn, cfg.user, "schema_table_count") ?? "0", 10),
      supplementalLogging: await getStatic(conn, cfg.user, "supplemental_logging"),
      archiveLagTarget: getArchiveLagTarget(await getStatic(conn, cfg.user, "archive_lag_target")),
      maxStringSize: getMaxStringSize(await getStatic(conn, cfg.user, "max_string_size")),
      captureSchema: await getStaticRaw(conn, cfg.user, "capture_schema") ?? "UNKNOWN",
      captureTablePattern: await getStaticRaw(conn, cfg.user, "capture_table_pattern") ?? "UNKNOWN",

      recommendations: {} as Recommendations,
    };

    diagReport.recommendations = computeRecommendations(diagReport);

    const md = generateMarkdown(diagReport);
    const env = generateEnvSnippet(diagReport.recommendations, diagReport);

    const outputDir = process.cwd();
    const mdPath = path.join(outputDir, "dbz-diag-report.md");
    const envPath = path.join(outputDir, "dbz-recommended.env");

    fs.writeFileSync(mdPath, md);
    fs.writeFileSync(envPath, env);

    console.log(`Report:  ${mdPath}`);
    console.log(`Config:  ${envPath}`);
    console.log(`\nSampling duration: ${duration.toFixed(1)} hours`);

    if (diagReport.recommendations.warnings.length > 0) {
      console.log("\n⚠ Warnings:");
      for (const w of diagReport.recommendations.warnings) {
        console.log(`  - ${w}`);
      }
    }
  });
}

// ── Metric queries ──────────────────────────────────────────────────────────

async function getSamplingDuration(conn: oracledb.Connection, user: string): Promise<number> {
  const rows = await queryRows<{ HOURS: number }>(conn, `
    SELECT ROUND((CAST(MAX(sample_time) AS DATE) - CAST(MIN(sample_time) AS DATE)) * 24, 2) AS HOURS
    FROM ${user}.${SAMPLE_TABLE}
  `);
  return rows[0]?.HOURS ?? 0;
}

async function getMetricStats(
  conn: oracledb.Connection,
  user: string,
  metricName: string,
  multiplier = 1
): Promise<MetricStats> {
  const rows = await queryRows<{ MIN_V: number; MAX_V: number; AVG_V: number; P95_V: number; CNT: number }>(conn, `
    SELECT
      MIN(metric_value) * ${multiplier} AS MIN_V,
      MAX(metric_value) * ${multiplier} AS MAX_V,
      ROUND(AVG(metric_value) * ${multiplier}, 2) AS AVG_V,
      ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric_value) * ${multiplier}, 2) AS P95_V,
      COUNT(*) AS CNT
    FROM ${user}.${SAMPLE_TABLE}
    WHERE metric_name = :name
  `, { name: metricName });

  const row = rows[0];
  return {
    min: row?.MIN_V ?? 0,
    max: row?.MAX_V ?? 0,
    avg: row?.AVG_V ?? 0,
    p95: row?.P95_V ?? 0,
    samples: row?.CNT ?? 0,
  };
}

async function getMetricAvg(conn: oracledb.Connection, user: string, metricName: string): Promise<number> {
  const rows = await queryRows<{ AVG_V: number }>(conn, `
    SELECT ROUND(AVG(metric_value), 2) AS AVG_V
    FROM ${user}.${SAMPLE_TABLE}
    WHERE metric_name = :name AND metric_value > 0
  `, { name: metricName });
  return rows[0]?.AVG_V ?? 0;
}

async function getStatic(conn: oracledb.Connection, user: string, checkName: string): Promise<any> {
  const raw = await getStaticRaw(conn, user, checkName);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

async function getStaticRaw(conn: oracledb.Connection, user: string, checkName: string): Promise<string | null> {
  const rows = await queryRows<{ CHECK_VALUE: string }>(conn, `
    SELECT check_value AS CHECK_VALUE FROM ${user}.${STATIC_TABLE} WHERE check_name = :name
  `, { name: checkName });
  return rows[0]?.CHECK_VALUE ?? null;
}

function getArchiveLagTarget(data: any): number {
  if (Array.isArray(data) && data.length > 0) return parseInt(data[0]?.VALUE ?? "0", 10);
  return 0;
}

function getMaxStringSize(data: any): string {
  if (Array.isArray(data) && data.length > 0) return data[0]?.VALUE ?? "STANDARD";
  return "STANDARD";
}

// ── Recommendation engine ───────────────────────────────────────────────────

function computeRecommendations(r: DiagReport): Recommendations {
  const warnings: string[] = [];
  const hasLobs = r.lobColumns && r.lobColumns.length > 0;

  // ── Redo log sizing ──
  // Target: 3-5 switches/hour at peak.
  // If current peak is already in range, keep current size.
  // If too many switches, recommend larger. If too few, keep (fewer is fine).
  const currentRedoSizeGb = r.redoLogConfig.length > 0
    ? r.redoLogConfig[0].BYTES / (1024 ** 3)
    : 4;
  const currentGroups = r.redoLogConfig.length;

  let redoLogSizeGb = currentRedoSizeGb;
  if (r.switchesPerHour.p95 > 6) {
    // Too many switches, need bigger logs
    const targetSwitches = 4;
    const peakGbPerHour = r.archiveGbPerHour.p95 || r.archiveGbPerHour.max;
    redoLogSizeGb = Math.ceil(peakGbPerHour / targetSwitches);
    redoLogSizeGb = Math.max(redoLogSizeGb, 2); // minimum 2GB
  }

  // Groups: minimum 4, one more than typical concurrent ACTIVE logs
  let redoLogGroups = Math.max(currentGroups, 4);

  // ── Archive retention ──
  // Must exceed: max observed transaction duration + archiving delay + safety buffer
  // With LOB disabled, transaction pinning shouldn't be an issue, but we account for
  // LogMiner session duration (proportional to archive file size) + RMAN race window.
  const archiveWriteTimeMin = (r.avgArchiveFileSizeGb / 0.5) * 1; // ~0.5 GB/s write speed estimate → minutes
  const logMinerSessionOverheadMin = 15; // conservative estimate for session referencing files
  const safetyBufferMin = 60;

  // The key constraint: retention > (time LogMiner might reference an old file)
  // With LOB off: this is roughly the archive write time + a couple of switch intervals
  const switchIntervalMinP95 = r.switchesPerHour.p95 > 0 ? 60 / r.switchesPerHour.p95 : 30;
  const minRetentionMin = Math.max(
    r.oldestTxnMinutes.p95 + safetyBufferMin,
    switchIntervalMinP95 * 3 + archiveWriteTimeMin + logMinerSessionOverheadMin + safetyBufferMin,
    120 // absolute minimum 2 hours
  );
  const archiveRetentionHours = Math.ceil(minRetentionMin / 60);

  const retentionDiskGb = r.archiveGbPerHour.p95 * archiveRetentionHours;

  // ── LOB ──
  let lobEnabled = false;
  let lobReason = "No LOB columns in captured tables.";
  if (hasLobs) {
    lobEnabled = false; // Still recommend off by default
    lobReason = `${r.lobColumns.length} LOB column(s) found in captured tables. ` +
      `LOB capture disabled by default to prevent watermark pinning. ` +
      `Enable only if LOB data capture is required AND retention is >= ${archiveRetentionHours + 1}h.`;
    warnings.push(
      `LOB columns detected: ${r.lobColumns.map((c: any) => `${c.TABLE_NAME}.${c.COLUMN_NAME}`).join(", ")}. ` +
      `If LOB capture is needed, consider VARCHAR2(32767) conversion (max_string_size=${r.maxStringSize}).`
    );
  }

  // ── Transaction retention ──
  // Must cover the full lifetime of the longest observed transaction.
  // LogMiner pins the watermark at the oldest open txn's start SCN; all
  // archive logs from that point forward must still exist.  Using p95 * 2
  // (uncapped) ensures Debezium doesn't abandon a legitimately long txn and
  // leave the watermark pinned longer than expected.  Minimum 5 min.
  const txnRetentionMs = Math.max(
    r.oldestTxnMinutes.p95 * 2 * 60 * 1000,
    300000
  );

  // ── Heartbeat ──
  // More frequent when archive retention is tight
  const heartbeatIntervalMs = archiveRetentionHours <= 2 ? 10000 : 30000;

  // ── Batch sizing ──
  const captureRatio = r.schemaTableCount > 0 ? r.capturedTableCount / r.schemaTableCount : 1;
  const batchSizeDefault = captureRatio < 0.3 ? 5000 : 10000;
  const batchSizeMax = captureRatio < 0.3 ? 10000 : 50000;

  // ── Query filter mode ──
  const queryFilterMode = captureRatio < 0.5 ? "regex" : "none";
  if (queryFilterMode === "regex") {
    warnings.push(
      `Captured tables are ${(captureRatio * 100).toFixed(0)}% of schema. ` +
      `query.filter.mode=regex recommended to reduce LogMiner overhead. ` +
      `Monitor that messages still arrive after enabling.`
    );
  }

  // ── Max retries ──
  // Scale with archive file size — bigger files need more retries during archiving
  const maxRetries = r.avgArchiveFileSizeGb > 5 ? 30 : 10;

  // ── Archive lag target check ──
  if (r.archiveLagTarget === 0 && r.switchesPerHour.min < 2) {
    warnings.push(
      `archive_lag_target is 0 and minimum switch rate is ${r.switchesPerHour.min}/hour. ` +
      `During quiet periods, long gaps without switches can stale the offset. ` +
      `Set archive_lag_target=1800 as a safety net.`
    );
  }

  // ── Supplemental logging check ──
  const suppLog = r.supplementalLogging;
  if (suppLog && Array.isArray(suppLog) && suppLog.length > 0) {
    const min = suppLog[0]?.SUPPLEMENTAL_LOG_DATA_MIN;
    if (min !== "YES") {
      warnings.push("Minimum supplemental logging is NOT enabled. Debezium requires at least minimal supplemental logging.");
    }
  }

  // ── Archive window sanity check (ORA-00308 risk) ──
  // archive_window_hours reflects how long archives are *actually* kept on
  // disk right now (non-deleted files spanning from oldest to newest).
  // If the observed minimum window is already below the recommended retention,
  // the current RMAN / cleanup policy is too aggressive and Debezium would hit
  // ORA-00308 today even before any tuning.
  if (r.archiveWindowHours.samples > 0 && r.archiveWindowHours.min < archiveRetentionHours) {
    warnings.push(
      `ORA-00308 RISK: observed minimum archive window is ${r.archiveWindowHours.min.toFixed(1)}h ` +
      `but recommended retention is ${archiveRetentionHours}h. ` +
      `The current cleanup policy is deleting archives too soon — LogMiner will lose files ` +
      `it still needs. Raise RMAN retention or reduce archive generation before enabling Debezium.`
    );
  }

  return {
    redoLogSizeGb: Math.round(redoLogSizeGb * 10) / 10,
    redoLogGroups,
    archiveRetentionHours,
    archiveRetentionDiskGb: Math.round(retentionDiskGb),
    lobEnabled,
    lobReason,
    transactionRetentionMs: txnRetentionMs,
    heartbeatIntervalMs,
    batchSizeDefault,
    batchSizeMax,
    maxRetries,
    queryFilterMode,
    archiveLogOnlyMode: false,
    warnings,
  };
}

// ── Output generators ───────────────────────────────────────────────────────

function generateMarkdown(r: DiagReport): string {
  const rec = r.recommendations;
  const lines: string[] = [];
  const ln = (s = "") => lines.push(s);

  ln("# Debezium Oracle CDC — Diagnostic Report");
  ln();
  ln(`Generated: ${new Date().toISOString()}`);
  ln(`Sampling duration: ${r.samplingDurationHours.toFixed(1)} hours (${r.switchesPerHour.samples} samples)`);
  ln(`Schema: ${r.captureSchema}, Table pattern: \`${r.captureTablePattern}\``);
  ln();

  ln("## Observed Metrics");
  ln();
  ln("### Log Switches (per hour)");
  ln(fmtStats(r.switchesPerHour));
  ln();

  ln("### Archive Generation (GB/hour)");
  ln(fmtStats(r.archiveGbPerHour));
  ln();

  ln(`### Average Archive File Size: ${r.avgArchiveFileSizeGb.toFixed(2)} GB`);
  ln();

  ln("### Longest Active Transaction (minutes)");
  ln(fmtStats(r.oldestTxnMinutes));
  ln();

  ln("### Active Transaction Count");
  ln(fmtStats(r.activeTxnCount));
  ln();

  ln("### Archive Window Available (hours)");
  ln(fmtStats(r.archiveWindowHours));
  const windowOk = r.archiveWindowHours.min >= r.recommendations.archiveRetentionHours;
  ln(`> Recommended retention: **${r.recommendations.archiveRetentionHours}h** — current minimum window: **${r.archiveWindowHours.min.toFixed(1)}h** — ${windowOk ? "✓ OK" : "⚠ TOO SHORT (ORA-00308 risk)"}`);
  ln();

  ln("### Archive Disk Used (GB)");
  ln(fmtStats(r.archiveDiskUsedGb));
  ln();

  ln("## Current Configuration");
  ln();
  ln(`| Setting | Value |`);
  ln(`|---------|-------|`);
  ln(`| Redo log groups | ${r.redoLogConfig.length} |`);
  if (r.redoLogConfig.length > 0) {
    ln(`| Redo log size | ${(r.redoLogConfig[0].BYTES / (1024 ** 3)).toFixed(1)} GB |`);
  }
  ln(`| Captured tables | ${r.capturedTableCount} of ${r.schemaTableCount} (${r.schemaTableCount > 0 ? ((r.capturedTableCount / r.schemaTableCount) * 100).toFixed(0) : 0}%) |`);
  ln(`| LOB columns in captured tables | ${r.lobColumns?.length ?? 0} |`);
  ln(`| archive_lag_target | ${r.archiveLagTarget} |`);
  ln(`| max_string_size | ${r.maxStringSize} |`);
  ln();

  if (r.lobColumns && r.lobColumns.length > 0) {
    ln("### LOB Columns in Captured Tables");
    ln();
    ln("| Table | Column | Type |");
    ln("|-------|--------|------|");
    for (const col of r.lobColumns) {
      ln(`| ${col.TABLE_NAME} | ${col.COLUMN_NAME} | ${col.DATA_TYPE} |`);
    }
    ln();
  }

  ln("## Recommendations");
  ln();

  ln("### Redo Logs");
  ln(`- Size: **${rec.redoLogSizeGb} GB** per group`);
  ln(`- Groups: **${rec.redoLogGroups}**`);
  ln();

  ln("### Archive Retention");
  ln(`- Retention: **${rec.archiveRetentionHours} hours**`);
  ln(`- Estimated disk needed: **~${rec.archiveRetentionDiskGb} GB**`);
  ln(`- RMAN delete clause: \`delete noprompt archivelog all completed before 'SYSDATE-${rec.archiveRetentionHours}/24';\``);
  ln();

  ln("### LOB Support");
  ln(`- Enabled: **${rec.lobEnabled}**`);
  ln(`- Reason: ${rec.lobReason}`);
  ln();

  ln("### Debezium Tuning");
  ln(`- transaction.retention.ms: **${rec.transactionRetentionMs}** (${rec.transactionRetentionMs / 60000} min)`);
  ln(`- heartbeat.interval.ms: **${rec.heartbeatIntervalMs}**`);
  ln(`- batch.size.default: **${rec.batchSizeDefault}**`);
  ln(`- batch.size.max: **${rec.batchSizeMax}**`);
  ln(`- errors.max.retries: **${rec.maxRetries}**`);
  ln(`- query.filter.mode: **${rec.queryFilterMode}**`);
  ln(`- archive.log.only.mode: **${rec.archiveLogOnlyMode}**`);
  ln();

  if (rec.warnings.length > 0) {
    ln("## ⚠ Warnings");
    ln();
    for (const w of rec.warnings) {
      ln(`- ${w}`);
    }
    ln();
  }

  ln("## RMAN Script (recommended)");
  ln();
  ln("```bash");
  ln("# Add disk safety check to archive cleanup script:");
  ln(`USAGE=$(df --output=pcent /orafra | tail -1 | tr -dc '0-9')`);
  ln(`if [ "$USAGE" -gt 85 ]; then`);
  ln(`  # Emergency: shorter retention to protect database`);
  ln(`  delete noprompt archivelog all completed before 'SYSDATE-2/24';`);
  ln(`else`);
  ln(`  # Normal: recommended retention`);
  ln(`  delete noprompt archivelog all completed before 'SYSDATE-${rec.archiveRetentionHours}/24';`);
  ln(`fi`);
  ln("```");
  ln();

  if (r.archiveLagTarget === 0 && r.switchesPerHour.min < 2) {
    ln("## Oracle Parameter Change");
    ln();
    ln("```sql");
    ln("ALTER SYSTEM SET ARCHIVE_LAG_TARGET = 1800 SCOPE=BOTH;");
    ln("```");
    ln();
  }

  return lines.join("\n");
}

function generateEnvSnippet(rec: Recommendations, r: DiagReport): string {
  const lines: string[] = [];
  const ln = (s = "") => lines.push(s);

  ln("# ============================================================================");
  ln(`# Debezium Oracle CDC — Recommended Configuration`);
  ln(`# Generated: ${new Date().toISOString()}`);
  ln(`# Based on ${r.samplingDurationHours.toFixed(1)} hours of diagnostic sampling`);
  ln(`# Schema: ${r.captureSchema}, Tables: ${r.captureTablePattern}`);
  ln("# ============================================================================");
  ln();

  ln("# --- Core ---");
  ln(`DEBEZIUM_SOURCE_ORACLE_LOB_ENABLED=${rec.lobEnabled}`);
  ln(`DEBEZIUM_SOURCE_LOG_MINING_ARCHIVE_LOG_ONLY_MODE=${rec.archiveLogOnlyMode}`);
  ln("DEBEZIUM_SOURCE_LOG_MINING_STRATEGY=online_catalog");
  ln("DEBEZIUM_SOURCE_LOG_MINING_BUFFER_TYPE=ehcache");
  ln(`DEBEZIUM_SOURCE_SCHEMA_INCLUDE_LIST=${r.captureSchema}`);
  ln(`DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST=${r.captureSchema}\\\\.${r.captureTablePattern}`);
  ln("DEBEZIUM_SOURCE_SNAPSHOT_MODE=no_data");
  ln("DEBEZIUM_SOURCE_INCLUDE_SCHEMA_CHANGES=false");
  ln();

  ln("# --- Transaction handling ---");
  ln(`DEBEZIUM_SOURCE_LOG_MINING_TRANSACTION_RETENTION_MS=${rec.transactionRetentionMs}`);
  ln();

  ln("# --- Heartbeat ---");
  ln(`DEBEZIUM_SOURCE_HEARTBEAT_INTERVAL_MS=${rec.heartbeatIntervalMs}`);
  ln(`DEBEZIUM_SOURCE_HEARTBEAT_ACTION_QUERY="UPDATE ${r.captureSchema}.<TABLE> SET <col> = <col> WHERE ROWNUM = 1"`);
  ln();

  ln("# --- Performance ---");
  ln(`DEBEZIUM_SOURCE_LOG_MINING_BATCH_SIZE_DEFAULT=${rec.batchSizeDefault}`);
  ln(`DEBEZIUM_SOURCE_LOG_MINING_BATCH_SIZE_MAX=${rec.batchSizeMax}`);
  if (rec.queryFilterMode !== "none") {
    ln(`DEBEZIUM_SOURCE_LOG_MINING_QUERY_FILTER_MODE=${rec.queryFilterMode}`);
  }
  ln();

  ln("# --- Error handling ---");
  ln(`DEBEZIUM_SOURCE_ERRORS_MAX_RETRIES=${rec.maxRetries}`);
  ln("DEBEZIUM_SOURCE_ERRORS_RETRY_DELAY_INITIAL_MS=1000");
  ln("DEBEZIUM_SOURCE_ERRORS_RETRY_DELAY_MAX_MS=30000");
  ln();

  ln("# --- Offset & flush ---");
  ln("DEBEZIUM_SOURCE_OFFSET_FLUSH_INTERVAL_MS=10000");
  ln();

  ln("# ============================================================================");
  ln("# DBA actions required:");
  ln(`# 1. Redo logs: ${rec.redoLogGroups} groups x ${rec.redoLogSizeGb}GB`);
  ln(`# 2. Archive retention: ${rec.archiveRetentionHours} hours (SYSDATE-${rec.archiveRetentionHours}/24)`);
  ln(`#    Estimated disk needed: ~${rec.archiveRetentionDiskGb}GB`);
  if (r.archiveLagTarget === 0) {
    ln("# 3. Set ARCHIVE_LAG_TARGET=1800");
  }
  ln("# ============================================================================");

  return lines.join("\n");
}

function fmtStats(s: MetricStats): string {
  return `| Metric | Value |\n|--------|-------|\n| Min | ${s.min} |\n| Avg | ${s.avg} |\n| P95 | ${s.p95} |\n| Max | ${s.max} |`;
}
