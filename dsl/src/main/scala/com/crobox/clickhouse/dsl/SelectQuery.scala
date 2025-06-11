package com.crobox.clickhouse.dsl

case class SelectQuery(columns: Seq[Column], modifier: String = "", distinctColumns: Option[Seq[Column]] = None)
    extends Query
    with OperationalQuery {
  override val internalQuery = InternalQuery(Some(this))

  def addColumn(column: Column): SelectQuery =
    if (columns.exists(_.name == column.name)) this else copy(columns = columns ++ Seq(column))

  def removeColumn(column: Column): SelectQuery =
    copy(columns = columns.filter(_.name != column.name))
}
