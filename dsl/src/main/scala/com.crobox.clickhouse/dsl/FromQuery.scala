package com.crobox.clickhouse.dsl

sealed trait FromQuery extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(from = Some(this))

  val alias: String
}

sealed case class InnerFromQuery(private[dsl] val innerQuery: OperationalQuery, alias: String) extends FromQuery

sealed case class TableFromQuery[T <: Table](table: T, alias: String) extends FromQuery
