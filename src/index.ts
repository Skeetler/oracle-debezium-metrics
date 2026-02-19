import { Command } from "commander";
import * as dotenv from "dotenv";
import { getDbConfig } from "./db";
import { setup } from "./setup";
import { report } from "./report";
import { teardown } from "./teardown";

dotenv.config();

const program = new Command();

program
  .name("dbz-oracle-diag")
  .description("Debezium Oracle CDC diagnostic tool — profiles a database and recommends optimal configuration")
  .version("1.0.0");

program
  .command("setup")
  .description("Create monitoring tables and start sampling job. Run once, then wait 24h+.")
  .action(async () => {
    try {
      await setup(getDbConfig());
    } catch (e: any) {
      console.error(`\n✗ Setup failed: ${e.message}`);
      process.exit(1);
    }
  });

program
  .command("report")
  .description("Generate diagnostic report and recommended .env from collected samples.")
  .action(async () => {
    try {
      await report(getDbConfig());
    } catch (e: any) {
      console.error(`\n✗ Report failed: ${e.message}`);
      process.exit(1);
    }
  });

program
  .command("teardown")
  .description("Remove all diagnostic tables and scheduler jobs.")
  .action(async () => {
    try {
      await teardown(getDbConfig());
    } catch (e: any) {
      console.error(`\n✗ Teardown failed: ${e.message}`);
      process.exit(1);
    }
  });

program.parse();
