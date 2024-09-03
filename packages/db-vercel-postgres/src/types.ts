import type {
  BasePostgresAdapter,
  GenericEnum,
  MigrateDownArgs,
  MigrateUpArgs,
  PostgresDB,
} from '@payloadcms/drizzle/postgres'
import type { DrizzleAdapter } from '@payloadcms/drizzle/types'
import type { VercelPool, VercelPostgresPoolConfig } from '@vercel/postgres'
import type { DrizzleConfig } from 'drizzle-orm'
import type { PgSchema, PgTableFn, PgTransactionConfig } from 'drizzle-orm/pg-core'

export type Args = {
  connectionString?: string
  idType?: 'serial' | 'uuid'
  localesSuffix?: string
  logger?: DrizzleConfig['logger']
  migrationDir?: string
  /**
   * Optional pool configuration for Vercel Postgres
   * If not provided, vercel/postgres will attempt to use the Vercel environment variables
   */
  pool?: VercelPostgresPoolConfig
  prodMigrations?: {
    down: (args: MigrateDownArgs) => Promise<void>
    name: string
    up: (args: MigrateUpArgs) => Promise<void>
  }[]
  push?: boolean
  relationshipsSuffix?: string
  /**
   * The schema name to use for the database
   * @experimental This only works when there are not other tables or enums of the same name in the database under a different schema. Awaiting fix from Drizzle.
   */
  schemaName?: string
  transactionOptions?: false | PgTransactionConfig
  versionsSuffix?: string
}

export type VercelPostgresAdapter = {
  pool?: VercelPool
  poolOptions?: Args['pool']
} & BasePostgresAdapter

declare module 'payload' {
  export interface DatabaseAdapter
    extends Omit<Args, 'idType' | 'logger' | 'migrationDir' | 'pool'>,
      DrizzleAdapter {
    beginTransaction: (options?: PgTransactionConfig) => Promise<null | number | string>
    drizzle: PostgresDB
    enums: Record<string, GenericEnum>
    /**
     * An object keyed on each table, with a key value pair where the constraint name is the key, followed by the dot-notation field name
     * Used for returning properly formed errors from unique fields
     */
    fieldConstraints: Record<string, Record<string, string>>
    idType: Args['idType']
    initializing: Promise<void>
    localesSuffix?: string
    logger: DrizzleConfig['logger']
    pgSchema?: { table: PgTableFn } | PgSchema
    pool: VercelPool
    poolOptions: Args['pool']
    prodMigrations?: {
      down: (args: MigrateDownArgs) => Promise<void>
      name: string
      up: (args: MigrateUpArgs) => Promise<void>
    }[]
    push: boolean
    rejectInitializing: () => void
    relationshipsSuffix?: string
    resolveInitializing: () => void
    schema: Record<string, unknown>
    schemaName?: Args['schemaName']
    tableNameMap: Map<string, string>
    versionsSuffix?: string
  }
}
