package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.language.TokenizerModule.Database

sealed trait FromQuery extends Query with OperationalQuery with JoinableQuery

sealed case class InnerFromQuery(select: SelectQuery, private[dsl] val innerQuery: OperationalQuery)
    extends FromQuery {
  override val internalQuery = InternalQuery(select, this)
}

sealed case class TableFromQuery[T <: Table](select: SelectQuery, table: T, altDb: Option[Database] = None)
    extends FromQuery {
  override val internalQuery = InternalQuery(select, this)
}
