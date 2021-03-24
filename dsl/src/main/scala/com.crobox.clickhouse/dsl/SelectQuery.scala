package com.crobox.clickhouse.dsl

case class SelectQuery(columns: Seq[Column], modifier: String = "") extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(Some(this))

  def addColumn(column: Column): SelectQuery =
    if (columns.exists(_.name == column.name)) copy(columns = columns ++ Seq(column)) else this

  def removeColumn(column: Column): SelectQuery =
    copy(columns = columns.filter(_.name != column.name))
}
