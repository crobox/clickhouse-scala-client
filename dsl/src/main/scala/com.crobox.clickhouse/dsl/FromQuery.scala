package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.misc.RandomStringGenerator

sealed trait FromQuery extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(from = Some(this))

  val alias: String
}

sealed case class InnerFromQuery(private[dsl] val innerQuery: OperationalQuery,
                                 alias: String = RandomStringGenerator.random())
    extends FromQuery

sealed case class TableFromQuery[T <: Table](table: T, _alias: Option[String] = None) extends FromQuery {
  override val alias: String = _alias.getOrElse(table.name)
}
