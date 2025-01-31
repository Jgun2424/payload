import type { LibSQLDatabase } from 'drizzle-orm/libsql'
import type { FlattenedJoinField, Sort, Where } from 'payload'

import toSnakeCase from 'to-snake-case'

import type { BuildQueryJoinAliases, ChainedMethods, DrizzleAdapter } from '../types.js'

import buildQuery from '../queries/buildQuery.js'
import { getTableAlias } from '../queries/getTableAlias.js'
import { getNameFromDrizzleTable } from '../utilities/getNameFromDrizzleTable.js'
import { rawConstraint } from '../utilities/rawConstraint.js'
import { chainMethods } from './chainMethods.js'

export const buildCollectionJoinQuery = ({
  adapter,
  collection,
  currentTableName,
  extraSelect,
  field,
  limit,
  locale,
  parentCollection,
  path,
  sort,
  versions,
  where,
}: {
  adapter: DrizzleAdapter
  collection: string
  currentTableName: string
  extraSelect?: any
  field: FlattenedJoinField
  limit?: number
  locale?: string
  parentCollection: string
  path: string
  sort?: Sort
  versions?: boolean
  where?: Where
}) => {
  const fields = adapter.payload.collections[collection].config.flattenedFields

  const joinCollectionTableName = adapter.tableNameMap.get(toSnakeCase(collection))

  const joins: BuildQueryJoinAliases = []

  const currentIDColumn = versions
    ? adapter.tables[currentTableName].parent
    : adapter.tables[currentTableName].id

  let joinQueryWhere: Where

  if (Array.isArray(field.targetField.relationTo)) {
    joinQueryWhere = {
      [field.on]: {
        equals: {
          relationTo: parentCollection,
          value: rawConstraint(currentIDColumn),
        },
      },
    }
  } else {
    joinQueryWhere = {
      [field.on]: {
        equals: rawConstraint(currentIDColumn),
      },
    }
  }

  if (where && Object.keys(where).length) {
    joinQueryWhere = {
      and: [joinQueryWhere, where],
    }
  }

  const columnName = `${path.replaceAll('.', '_')}${field.name}`

  const subQueryAlias = `${columnName}_alias`

  const { newAliasTable } = getTableAlias({
    adapter,
    tableName: joinCollectionTableName,
  })

  const {
    orderBy,
    selectFields,
    where: subQueryWhere,
  } = buildQuery({
    adapter,
    aliasTable: newAliasTable,
    fields,
    joins,
    locale,
    selectLocale: true,
    sort,
    tableName: joinCollectionTableName,
    where: joinQueryWhere,
  })

  const chainedMethods: ChainedMethods = []

  joins.forEach(({ type, condition, table }) => {
    chainedMethods.push({
      args: [table, condition],
      method: type ?? 'leftJoin',
    })
  })

  if (limit !== 0) {
    chainedMethods.push({
      args: [limit],
      method: 'limit',
    })
  }

  const db = adapter.drizzle as LibSQLDatabase

  for (let key in selectFields) {
    const val = selectFields[key]

    if (val.table && getNameFromDrizzleTable(val.table) === joinCollectionTableName) {
      delete selectFields[key]
      key = key.split('.').pop()
      selectFields[key] = newAliasTable[key]
    }
  }

  if (extraSelect) {
    for (const k in extraSelect) {
      selectFields[k] = extraSelect[k]
    }
  }

  const subQuery = chainMethods({
    methods: chainedMethods,
    query: db
      .select(selectFields as any)
      .from(newAliasTable)
      .where(subQueryWhere)
      .orderBy(() => orderBy.map(({ column, order }) => order(column))),
  }).as(subQueryAlias)

  return { selectFields, subQuery, subQueryAlias }
}
