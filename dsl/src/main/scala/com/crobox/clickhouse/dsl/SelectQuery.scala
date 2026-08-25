package com.crobox.clickhouse.dsl

case class SelectQuery(columns: Seq[Column], modifier: String = "", distinctColumns: Option[Seq[Column]] = None)
    extends Query
    with OperationalQuery {
  override val internalQuery = InternalQuery(Some(this))

  // Compared by value, not by name: an unaliased expression takes its target column's name, so `sum(x)` and `uniq(x)`
  // are both called `x` and only one of them survived a name comparison.
  def addColumn(column: Column): SelectQuery =
    if (columns.contains(column)) this else copy(columns = columns ++ Seq(column))

  def removeColumn(column: Column): SelectQuery =
    copy(columns = columns.filterNot(_ == column))
}
