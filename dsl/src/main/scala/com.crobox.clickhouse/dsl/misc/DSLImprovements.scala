package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl.{Column, OperationalQuery}

object DSLImprovements {

  implicit class ColumnsImprv[T <: Column](columns: Seq[T]) {

    def addColumn(column: T): Seq[T] =
      if (columns.exists(_.name == column.name)) columns else columns ++ Seq(column)

    def +++(column: T): Seq[T] = addColumn(column)

    def removeColumn(column: T): Seq[T] = columns.filter(_.name != column.name)

    def removeColumn(column: String): Seq[T] = columns.filter(_.name != column)

    def ---(column: String): Seq[T] = removeColumn(column)
  }

  implicit class OperationalQueryImpr(query: OperationalQuery) {

    def selectColumns(): Seq[Column] = query.internalQuery.select.map(_.columns).getOrElse(Seq.empty)

    def addSelectColumn[T <: Column](column: T): OperationalQuery =
      query.select(selectColumns().addColumn(column): _*)

    def removeSelectColumn[T <: Column](column: T): OperationalQuery =
      query.select(selectColumns().removeColumn(column): _*)
  }
}
