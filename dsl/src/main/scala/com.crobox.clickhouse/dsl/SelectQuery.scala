package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

import scala.collection.mutable

case class SelectQuery(columns: mutable.LinkedHashSet[AnyTableColumn], modifier: String = "") extends Query {
  override val underlying = UnderlyingQuery(this, null)

  def from[T <: Table](table: T): FromQuery =
    TableFromQuery(this, table)

  def from(query: OperationalQuery): FromQuery =
    InnerFromQuery(this, query)

}
