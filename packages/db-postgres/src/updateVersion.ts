import type { TypeWithVersion, UpdateVersionArgs } from 'payload/database'
import type { PayloadRequest, SanitizedCollectionConfig, TypeWithID } from 'payload/types'

import { buildVersionCollectionFields } from 'payload/versions'

import type { PostgresAdapter } from './types'

import buildQuery from './queries/buildQuery'
import { upsertRow } from './upsertRow'
import { getTableName } from './utilities/getTableName'

export async function updateVersion<T extends TypeWithID>(
  this: PostgresAdapter,
  {
    id,
    collection,
    locale,
    req = {} as PayloadRequest,
    versionData,
    where: whereArg,
  }: UpdateVersionArgs<T>,
) {
  const db = this.sessions[req.transactionID]?.db || this.drizzle
  const collectionConfig: SanitizedCollectionConfig = this.payload.collections[collection].config
  const whereToUse = whereArg || { id: { equals: id } }
  const tableName = `_${getTableName(collectionConfig)}_v`
  const fields = buildVersionCollectionFields(collectionConfig)

  const { where } = await buildQuery({
    adapter: this,
    fields,
    locale,
    tableName,
    where: whereToUse,
  })

  const result = await upsertRow<TypeWithVersion<T>>({
    id,
    adapter: this,
    data: versionData,
    db,
    fields,
    operation: 'update',
    req,
    tableName,
    where,
  })

  return result
}
