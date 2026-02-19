import oracledb from "oracledb";
import { DbConfig, withConnection, execute, queryRows } from "./db";

const SAMPLE_TABLE = "DBZ_DIAG_SAMPLES";
const STATIC_TABLE = "DBZ_DIAG_STATIC";
const JOB_NAME = "DBZ_DIAG_SAMPLER";

export async function setup(cfg: DbConfig): Promise<void> {
  const schema = process.env.CAPTURE_SCHEMA;
  const tablePattern = process.env.CAPTURE_TABLE_PATTERN;
  const intervalMin = parseInt(process.env.SAMPLE_INTERVAL_MINUTES ?? "15", 10);

  if (!schema || !tablePattern) {
    throw new Error("Missing CAPTURE_SCHEMA or CAPTURE_TABLE_PATTERN");
  }

  await withConnection(cfg, async (conn) => {
    console.log("Checking privileges...");
    await checkPrivileges(conn, cfg.user);

    console.log("Creating monitoring tables...");
    await createTables(conn, cfg.user);

    console.log("Collecting static diagnostics...");
    await collectStatic(conn, cfg.user, schema, tablePattern);

    console.log(`Creating sampler job (every ${intervalMin} min)...`);
    await createSamplerJob(conn, cfg.user, intervalMin);

    console.log("\n✓ Setup complete.");
    console.log(`  Sampling every ${intervalMin} minutes into ${cfg.user}.${SAMPLE_TABLE}`);
    console.log(`  Static data in ${cfg.user}.${STATIC_TABLE}`);
    console.log(`  Let it run for at least 24 hours (ideally a full business day).`);
    console.log(`  Then run: npm run report`);
  });
}

async function checkPrivileges(conn: oracledb.Connection, user: string): Promise<void> {
  const checks = [
    { name: "v$archived_log", sql: "SELECT 1 FROM v$archived_log WHERE ROWNUM = 1" },
    { name: "v$log", sql: "SELECT 1 FROM v$log WHERE ROWNUM = 1" },
    { name: "v$database", sql: "SELECT 1 FROM v$database" },
    { name: "v$transaction", sql: "SELECT 1 FROM v$transaction WHERE ROWNUM = 1" },
    { name: "v$parameter", sql: "SELECT 1 FROM v$parameter WHERE ROWNUM = 1" },
    { name: "v$session", sql: "SELECT 1 FROM v$session WHERE ROWNUM = 1" },
  ];

  const missing: string[] = [];
  for (const check of checks) {
    try {
      await conn.execute(check.sql);
    } catch {
      missing.push(check.name);
    }
  }

  if (missing.length > 0) {
    console.error(`\n✗ Missing SELECT privileges on: ${missing.join(", ")}`);
    console.error(`  Grant with (as SYS):`);
    for (const view of missing) {
      console.error(`    GRANT SELECT ON ${view} TO ${user};`);
    }
    throw new Error("Insufficient privileges");
  }
  console.log("  All required privileges present.");
}

async function createTables(conn: oracledb.Connection, user: string): Promise<void> {
  // Drop existing if present
  for (const table of [SAMPLE_TABLE, STATIC_TABLE]) {
    try {
      await execute(conn, `DROP TABLE ${user}.${table} PURGE`);
    } catch {
      // Table doesn't exist, fine
    }
  }

  await execute(conn, `
    CREATE TABLE ${user}.${SAMPLE_TABLE} (
      sample_time  TIMESTAMP DEFAULT SYSTIMESTAMP,
      metric_name  VARCHAR2(100),
      metric_value NUMBER
    )
  `);

  await execute(conn, `
    CREATE TABLE ${user}.${STATIC_TABLE} (
      check_time  TIMESTAMP DEFAULT SYSTIMESTAMP,
      check_name  VARCHAR2(100),
      check_value VARCHAR2(4000)
    )
  `);
}

async function collectStatic(
  conn: oracledb.Connection,
  user: string,
  schema: string,
  tablePattern: string
): Promise<void> {
  // Redo log configuration
  const redoLogs = await queryRows<{ GROUP_NUM: number; BYTES: number; MEMBERS: number; STATUS: string }>(
    conn,
    `SELECT group# AS GROUP_NUM, bytes AS BYTES, members AS MEMBERS, status AS STATUS FROM v$log ORDER BY group#`
  );
  await insertStatic(conn, user, "redo_log_config", JSON.stringify(redoLogs));

  // Archive destination
  const archDest = await queryRows(
    conn,
    `SELECT dest_name, status, destination FROM v$archive_dest WHERE status = 'VALID' AND ROWNUM <= 5`
  );
  await insertStatic(conn, user, "archive_destinations", JSON.stringify(archDest));

  // FRA config
  const fra = await queryRows(
    conn,
    `SELECT name, value FROM v$parameter WHERE name LIKE 'db_recovery%'`
  );
  await insertStatic(conn, user, "fra_config", JSON.stringify(fra));

  // Archive lag target
  const lagTarget = await queryRows(
    conn,
    `SELECT value FROM v$parameter WHERE name = 'archive_lag_target'`
  );
  await insertStatic(conn, user, "archive_lag_target", JSON.stringify(lagTarget));

  // Supplemental logging
  const suppLog = await queryRows(
    conn,
    `SELECT supplemental_log_data_min, supplemental_log_data_pk, supplemental_log_data_all FROM v$database`
  );
  await insertStatic(conn, user, "supplemental_logging", JSON.stringify(suppLog));

  // CLOB/BLOB columns in captured tables
  const lobCols = await queryRows(
    conn,
    `SELECT table_name, column_name, data_type
     FROM all_tab_columns
     WHERE owner = :schema
       AND REGEXP_LIKE(table_name, :pattern)
       AND data_type IN ('CLOB', 'BLOB', 'NCLOB')
     ORDER BY table_name, column_name`,
    { schema, pattern: tablePattern }
  );
  await insertStatic(conn, user, "lob_columns", JSON.stringify(lobCols));

  // Count of captured tables
  const tableCount = await queryRows<{ CNT: number }>(
    conn,
    `SELECT COUNT(*) AS CNT FROM all_tables
     WHERE owner = :schema AND REGEXP_LIKE(table_name, :pattern)`,
    { schema, pattern: tablePattern }
  );
  await insertStatic(conn, user, "captured_table_count", String(tableCount[0]?.CNT ?? 0));

  // Total tables in schema (for ratio)
  const allTableCount = await queryRows<{ CNT: number }>(
    conn,
    `SELECT COUNT(*) AS CNT FROM all_tables WHERE owner = :schema`,
    { schema }
  );
  await insertStatic(conn, user, "schema_table_count", String(allTableCount[0]?.CNT ?? 0));

  // max_string_size (for CLOB→VARCHAR2 recommendation)
  const maxStr = await queryRows(
    conn,
    `SELECT value FROM v$parameter WHERE name = 'max_string_size'`
  );
  await insertStatic(conn, user, "max_string_size", JSON.stringify(maxStr));

  // Capture config for the report
  await insertStatic(conn, user, "capture_schema", schema);
  await insertStatic(conn, user, "capture_table_pattern", tablePattern);
}

async function createSamplerJob(conn: oracledb.Connection, user: string, intervalMin: number): Promise<void> {
  // Drop existing job if present
  try {
    await execute(conn, `BEGIN DBMS_SCHEDULER.DROP_JOB('${JOB_NAME}', TRUE); END;`);
  } catch {
    // Job doesn't exist
  }

  const plsql = `
    BEGIN
      -- Log switches in last sampling interval
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'switches', COUNT(*)
      FROM v$archived_log
      WHERE first_time > SYSDATE - ${intervalMin}/1440
        AND resetlogs_change# = (SELECT resetlogs_change# FROM v$database);

      -- Archive GB generated in last sampling interval
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'archive_gb', NVL(SUM(blocks * block_size) / 1024 / 1024 / 1024, 0)
      FROM v$archived_log
      WHERE first_time > SYSDATE - ${intervalMin}/1440
        AND resetlogs_change# = (SELECT resetlogs_change# FROM v$database);

      -- Average archive file size in GB (from last interval)
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'avg_archive_size_gb', NVL(AVG(blocks * block_size) / 1024 / 1024 / 1024, 0)
      FROM v$archived_log
      WHERE first_time > SYSDATE - ${intervalMin}/1440
        AND resetlogs_change# = (SELECT resetlogs_change# FROM v$database);

      -- Current SCN
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'current_scn', current_scn FROM v$database;

      -- Oldest active transaction age (minutes)
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'oldest_txn_mins', NVL(MAX(ROUND((SYSDATE - t.start_date) * 24 * 60)), 0)
      FROM v$transaction t;

      -- Active transaction count
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'active_txn_count', COUNT(*)
      FROM v$transaction;

      -- Archive files with deleted=NO (available window)
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'archive_window_hours',
             NVL(ROUND((MAX(next_time) - MIN(first_time)) * 24, 2), 0)
      FROM v$archived_log
      WHERE deleted = 'NO'
        AND resetlogs_change# = (SELECT resetlogs_change# FROM v$database);

      -- Total archive disk usage (deleted=NO files)
      INSERT INTO ${user}.${SAMPLE_TABLE} (metric_name, metric_value)
      SELECT 'archive_disk_used_gb',
             NVL(SUM(blocks * block_size) / 1024 / 1024 / 1024, 0)
      FROM v$archived_log
      WHERE deleted = 'NO'
        AND resetlogs_change# = (SELECT resetlogs_change# FROM v$database);

      COMMIT;
    END;
  `;

  await execute(conn, `
    BEGIN
      DBMS_SCHEDULER.CREATE_JOB (
        job_name        => '${JOB_NAME}',
        job_type        => 'PLSQL_BLOCK',
        job_action      => q'[${plsql}]',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=MINUTELY; INTERVAL=${intervalMin}',
        enabled         => TRUE
      );
    END;
  `);

  // Run once immediately to have initial data
  await execute(conn, `BEGIN DBMS_SCHEDULER.RUN_JOB('${JOB_NAME}'); END;`);
  console.log("  Initial sample collected.");
}

async function insertStatic(conn: oracledb.Connection, user: string, name: string, value: string): Promise<void> {
  await execute(
    conn,
    `INSERT INTO ${user}.${STATIC_TABLE} (check_name, check_value) VALUES (:name, :value)`,
    { name, value }
  );
}
