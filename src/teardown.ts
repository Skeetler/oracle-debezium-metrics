import { DbConfig, withConnection, execute } from "./db";

const SAMPLE_TABLE = "DBZ_DIAG_SAMPLES";
const STATIC_TABLE = "DBZ_DIAG_STATIC";
const JOB_NAME = "DBZ_DIAG_SAMPLER";

export async function teardown(cfg: DbConfig): Promise<void> {
  await withConnection(cfg, async (conn) => {
    console.log("Removing diagnostic objects...");

    try {
      await execute(conn, `BEGIN DBMS_SCHEDULER.DROP_JOB('${JOB_NAME}', TRUE); END;`);
      console.log(`  Dropped job ${JOB_NAME}`);
    } catch {
      console.log(`  Job ${JOB_NAME} not found (already removed)`);
    }

    for (const table of [SAMPLE_TABLE, STATIC_TABLE]) {
      try {
        await execute(conn, `DROP TABLE ${cfg.user}.${table} PURGE`);
        console.log(`  Dropped table ${table}`);
      } catch {
        console.log(`  Table ${table} not found (already removed)`);
      }
    }

    console.log("\n✓ Teardown complete.");
  });
}
