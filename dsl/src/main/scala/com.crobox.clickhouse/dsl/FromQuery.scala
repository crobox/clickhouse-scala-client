package com.crobox.clickhouse.dsl

sealed trait FromQuery extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(from = Some(this))

  val alias: Option[String]
}

sealed case class InnerFromQuery(private[dsl] val innerQuery: OperationalQuery, alias: Option[String] = None) extends FromQuery

sealed case class TableFromQuery[T <: Table](table: T) extends FromQuery {
  override val alias: Option[String] = Option(table.name)
}
