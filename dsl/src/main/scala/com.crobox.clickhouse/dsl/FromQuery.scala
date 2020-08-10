package com.crobox.clickhouse.dsl

sealed trait FromQuery extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(from = Some(this))
  val alias: Option[String]
  val finalized: Boolean
}

sealed case class InnerFromQuery(private[dsl] val innerQuery: OperationalQuery, alias: Option[String] = None)
    extends FromQuery {

  /** Queries can never have 'final' clause: Illegal FINAL */
  override val finalized = false
}

sealed case class TableFromQuery[T <: Table](table: T, alias: Option[String] = None, finalized: Boolean = false)
    extends FromQuery
