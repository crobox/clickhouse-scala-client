package com.crobox.clickhouse.dsl

/**
 * QueryFactory exposes all methods of OperationalQuery from a empty starting point (factoring new queries)
 */
trait QueryFactory extends OperationalQuery {
  override val internalQuery: InternalQuery = InternalQuery()
}
