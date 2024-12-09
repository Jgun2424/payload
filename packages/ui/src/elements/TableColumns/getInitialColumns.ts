import type { ClientField, Field } from 'payload'

import { fieldAffectsData } from 'payload/shared'

import type { ColumnPreferences } from '../../providers/ListQuery/index.js'

const getRemainingColumns = <T extends ClientField[] | Field[]>(
  fields: T,
  useAsTitle: string,
): ColumnPreferences =>
  fields?.reduce((remaining, field) => {
    if (fieldAffectsData(field) && field.name === useAsTitle) {
      return remaining
    }

    if (!fieldAffectsData(field) && 'fields' in field) {
      return [...remaining, ...getRemainingColumns(field.fields, useAsTitle)]
    }

    if (field.type === 'tabs' && 'tabs' in field) {
      return [
        ...remaining,
        ...field.tabs.reduce(
          (tabFieldColumns, tab) => [
            ...tabFieldColumns,
            ...('name' in tab ? [tab.name] : getRemainingColumns(tab.fields, useAsTitle)),
          ],
          [],
        ),
      ]
    }

    return [...remaining, field.name]
  }, [])

export const getInitialColumns = <T extends ClientField[] | Field[]>(
  fields: T,
  useAsTitle: string,
  defaultColumns: string[],
): ColumnPreferences => {
  let initialColumns = []

  if (Array.isArray(defaultColumns) && defaultColumns.length >= 1) {
    initialColumns = defaultColumns
  } else {
    if (useAsTitle) {
      initialColumns.push(useAsTitle)
    }

    const remainingColumns = getRemainingColumns(fields, useAsTitle)

    initialColumns = initialColumns.concat(remainingColumns)
    initialColumns = initialColumns.slice(0, 4)
  }

  return initialColumns.map((column) => ({
    accessor: column,
    active: true,
  }))
}
