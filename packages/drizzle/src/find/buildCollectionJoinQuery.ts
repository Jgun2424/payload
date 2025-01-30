import type { FlattenedField, FlattenedJoinField, JoinField, Where } from 'payload'

import toSnakeCase from 'to-snake-case'

import type { BuildQueryJoinAliases, DrizzleAdapter } from '../types.js'

import { rawConstraint } from '../utilities/rawConstraint.js'

export const buildCollectionJoinQuery = ({
  adapter,
  collection,
  currentTableName,
  field,
  parentCollection,
  versions,
}: {
  adapter: DrizzleAdapter
  collection: string
  currentTableName: string
  field: FlattenedJoinField
  parentCollection: string
  rootCollection: string
  versions?: boolean
}) => {
  const fields = adapter.payload.collections[collection].config.flattenedFields

  const joinCollectionTableName = adapter.tableNameMap.get(toSnakeCase(field.collection))

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

  const subQuery = chainMethods({
    methods: chainedMethods,
    query: db
      .select(selectFields as any)
      .from(newAliasTable)
      .where(subQueryWhere)
      .orderBy(() => orderBy.map(({ column, order }) => order(column))),
  }).as(subQueryAlias)

  currentArgs.extras[columnName] = sql`${db
    .select({
      result: jsonAggBuildObject(adapter, {
        id: sql.raw(`"${subQueryAlias}".id`),
        ...(selectFields._locale && {
          locale: sql.raw(`"${subQueryAlias}".${selectFields._locale.name}`),
        }),
      }),
    })
    .from(sql`${subQuery}`)}`.as(subQueryAlias)
}
