package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl._

object DSLImprovements {

  implicit class ColumnsImprovements(values: Seq[Column]) {

    //
    // Add
    //

    def addColumn(name: String, column: Column): Seq[Column] =
      if (values.exists(_.name == name)) values else values ++ Seq(column)

    def addColumn(column: Column): Seq[Column] = addColumn(column.name, column)

    def addColumns(column: Column, columns: Column*): Seq[Column] =
      addColumns(Iterable(column) ++ columns.toSeq)

    def addColumns(columns: Iterable[Column]): Seq[Column] =
      columns.foldLeft(values)((result, c) => result.addColumn(c))

    def +++(columns: Iterable[Column]): Seq[Column] = addColumns(columns)

    //
    // Remove
    //

    def removeColumn(column: Column): Seq[Column] = removeColumns(Iterable(column.name))

    def removeColumn(column: String): Seq[Column] = removeColumns(Iterable(column))

    def removeColumns(column: Column, columns: Column*): Seq[Column] =
      removeColumns(Iterable(column.name) ++ columns.map(_.name))

    def removeColumns(column: String, columns: String*): Seq[Column] = removeColumns(Iterable(column) ++ columns.toSeq)

    def removeColumns(columns: Iterable[String]): Seq[Column] =
      columns.foldLeft(values)((result, c) => result.filter(_.name != c))

    def ---(columns: Iterable[Column]): Seq[Column] = removeColumns(columns.map(_.name))

    //
    // replace
    //

    def replaceColumn(column: Column): Seq[Column] = replaceColumn(column.name, column)

    def replaceColumn(name: String, column: Column): Seq[Column] =
      values.indexWhere(_.name == name) match {
        case -1 => values ++ Seq(column)
        case 0 => Seq(column) ++ values.slice(1, values.size)
        case idx => values.slice(0, idx) ++ Seq(column) ++ values.slice(idx + 1, values.size)
      }
  }

  implicit class OperationalQueryImpr(query: OperationalQuery) {

    def selectColumns(): Seq[Column] = query.internalQuery.select.map(_.columns).getOrElse(Seq.empty)

    // Sort columns afterwards
    def insertSelectColumn[T <: Column](column: T): OperationalQuery =
      query.select(selectColumns().addColumn(column).sortBy(_.name): _*)

    // Sort columns afterwards
    def insertSelectColumn[T <: Column](column: Option[T]): OperationalQuery =
      column.map(insertSelectColumn(_)).getOrElse(query)

    // Sort columns afterwards
    def insertSelectColumn[T <: Column](column: T, filter: Boolean): OperationalQuery =
      if (filter) insertSelectColumn(column) else query

    def addSelectColumn[T <: Column](column: T): OperationalQuery =
      query.select(selectColumns().addColumn(column): _*)

    def addSelectColumn[T <: Column](column: Option[T]): OperationalQuery =
      column.map(addSelectColumn(_)).getOrElse(query)

    def addSelectColumn[T <: Column](column: T, filter: Boolean): OperationalQuery =
      if (filter) addSelectColumn(column) else query

    def removeSelectColumn[T <: Column](column: T): OperationalQuery =
      query.select(selectColumns().removeColumn(column): _*)

    def removeSelectColumn[T <: Column](column: Option[T]): OperationalQuery =
      column.map(removeSelectColumn(_)).getOrElse(query)

    def removeSelectColumn[T <: Column](column: T, filter: Boolean): OperationalQuery =
      if (filter) removeSelectColumn(column) else query

    def selectConstraints(): Option[TableColumn[Boolean]] = query.internalQuery.where

    def andConstraint(condition: Option[ExpressionColumn[Boolean]]): OperationalQuery =
      condition.map(andConstraint).getOrElse(query)

    def andConstraint(condition: ExpressionColumn[Boolean]): OperationalQuery = {
      val comparison = query.internalQuery.where.map(_.and(condition)).getOrElse(condition)
      OperationalQuery(query.internalQuery.copy(where = Some(comparison)))
    }

    def orConstraint(condition: Option[ExpressionColumn[Boolean]]): OperationalQuery =
      condition.map(orConstraint).getOrElse(query)

    def orConstraint(condition: ExpressionColumn[Boolean]): OperationalQuery = {
      val comparison = query.internalQuery.where.map(_.or(condition)).getOrElse(condition)
      OperationalQuery(query.internalQuery.copy(where = Some(comparison)))
    }

    def selectFromTable[T <: Table](): Option[T] = query.internalQuery.from.flatMap {
      case _: InnerFromQuery => None
      case x: TableFromQuery[_] => Option(x.table.asInstanceOf[T])
    }

    def insertConstraint(condition: Option[ExpressionColumn[Boolean]]): OperationalQuery =
      condition.map(insertConstraint).getOrElse(query)

    def insertConstraint(condition: ExpressionColumn[Boolean]): OperationalQuery = {
      query.internalQuery.from.foreach {
        case l1: InnerFromQuery =>
          return query.from(l1.innerQuery.andConstraint(condition))

        // this might be even another level 'deep'
        // See FilterQueryTransformerFourTableTest#
        //          l1.internalQuery.from.foreach {
        //            case l2: InnerFromQuery   => return query.from(l1.innerQuery.from(l2.innerQuery.andConstraint(condition)))
        //            case _: TableFromQuery[_] => return query.from(l1.innerQuery.andConstraint(condition))
        //            case _                    =>
        //          }
        case _: TableFromQuery[_] => return query.andConstraint(condition)
        case null =>
      }
      query
    }
  }

  implicit class OrderingColumnsImprovements(values: Seq[(Column, OrderingDirection)]) {

    def addColumns(columns: Iterable[(Column, OrderingDirection)]): Seq[(Column, OrderingDirection)] =
      columns.foldLeft(values)((result, c) => if (result.exists(_._1.name == c._1.name)) result else result ++ Seq(c))

    def +++(columns: Iterable[(Column, OrderingDirection)]): Seq[(Column, OrderingDirection)] = addColumns(columns)
  }
}
