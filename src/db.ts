import oracledb from "oracledb";

export interface DbConfig {
  host: string;
  port: number;
  service: string;
  user: string;
  password: string;
}

export function getDbConfig(): DbConfig {
  const required = ["ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "ORACLE_PASSWORD"];
  for (const key of required) {
    if (!process.env[key]) throw new Error(`Missing env var: ${key}`);
  }
  return {
    host: process.env.ORACLE_HOST!,
    port: parseInt(process.env.ORACLE_PORT!, 10),
    service: process.env.ORACLE_SERVICE!,
    user: process.env.ORACLE_USER!,
    password: process.env.ORACLE_PASSWORD!,
  };
}

export function getConnectString(cfg: DbConfig): string {
  return `${cfg.host}:${cfg.port}/${cfg.service}`;
}

const PRIVILEGE_MAP: Record<string, number> = {
  SYSDBA: oracledb.SYSDBA,
  SYSOPER: oracledb.SYSOPER,
};

export async function withConnection<T>(cfg: DbConfig, fn: (conn: oracledb.Connection) => Promise<T>): Promise<T> {
  oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
  const privilegeKey = process.env.ORACLE_PRIVILEGE?.toUpperCase();
  const conn = await oracledb.getConnection({
    user: cfg.user,
    password: cfg.password,
    connectString: getConnectString(cfg),
    ...(privilegeKey && PRIVILEGE_MAP[privilegeKey] ? { privilege: PRIVILEGE_MAP[privilegeKey] } : {}),
  });
  try {
    return await fn(conn);
  } finally {
    await conn.close();
  }
}

export async function execute(conn: oracledb.Connection, sql: string, binds: oracledb.BindParameters = {}): Promise<oracledb.Result<any>> {
  return conn.execute(sql, binds, { autoCommit: true });
}

export async function queryRows<T = Record<string, any>>(conn: oracledb.Connection, sql: string, binds: oracledb.BindParameters = {}): Promise<T[]> {
  const result = await conn.execute<T>(sql, binds);
  return (result.rows ?? []) as T[];
}
