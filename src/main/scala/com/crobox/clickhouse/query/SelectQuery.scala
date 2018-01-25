package com.crobox.clickhouse.query

import com.crobox.clickhouse.query.TableColumn.AnyTableColumn

import scala.collection.mutable

case class SelectQuery(columns: mutable.LinkedHashSet[AnyTableColumn], modifier: String = "") extends Query {
  override val underlying = UnderlyingQuery(this, null)

  def from[T <: Table](table: T): FromQuery =
    TableFromQuery(this, table)

  def from(query: OperationalQuery): FromQuery =
    InnerFromQuery(this, query)

}
