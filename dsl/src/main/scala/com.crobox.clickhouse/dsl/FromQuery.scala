package com.crobox.clickhouse.dsl

sealed trait FromQuery extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(from = Some(this))
}

sealed case class InnerFromQuery(private[dsl] val innerQuery: OperationalQuery) extends FromQuery

sealed case class TableFromQuery[T <: Table](table: T, altDb: Option[Any] = None, fromFinal: Boolean = false) extends FromQuery