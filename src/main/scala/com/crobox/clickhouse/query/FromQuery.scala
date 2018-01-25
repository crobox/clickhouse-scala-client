package com.crobox.clickhouse.query

import com.crobox.clickhouse.query.TokenizerModule.Database

sealed trait FromQuery extends Query with OperationalQuery with JoinableQuery

sealed case class InnerFromQuery(select: SelectQuery, private[query] val innerQuery: OperationalQuery)
    extends FromQuery {
  override val underlying = UnderlyingQuery(select, this)
}

sealed case class TableFromQuery[T <: Table](select: SelectQuery, table: T, altDb: Option[Database] = None)
    extends FromQuery {
  override val underlying = UnderlyingQuery(select, this)
}
