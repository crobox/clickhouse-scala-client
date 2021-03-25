package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl.{Column, OperationalQuery}

object DSLImprovements {

  implicit class ColumnsImprv[T <: Column](columns: Iterable[T]) {

    def addColumn(column: T): Iterable[T] =
      if (columns.exists(_.name == column.name)) columns else columns ++ Seq(column)

    def removeColumn(column: T): Iterable[T] = columns.filter(_.name != column.name)

    def removeColumn(column: String): Iterable[T] = columns.filter(_.name != column)
  }

  implicit class OperationalQueryImpr(query: OperationalQuery) {

    def selectColumns(): Seq[Column] = query.internalQuery.select.map(_.columns).getOrElse(Seq.empty)
  }
}
