package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl.Column

object ColumnImprovements {

  implicit class ColumnsImprv[T <: Column](columns: Iterable[T]) {

    def addColumn(column: T): Iterable[T] =
      if (columns.exists(_.name == column.name)) columns else columns ++ Seq(column)

    def removeColumn(column: T): Iterable[T] =
      columns.filter(_.name != column.name)
  }
}
