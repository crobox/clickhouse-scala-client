package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

import scala.collection.mutable

trait OperationalQuery extends Query with JoinableQuery {

  def where(condition: Comparison): OperationalQuery = {
    val comparison = internalQuery.where.map(_.and(condition)).getOrElse(condition)
    OperationalQueryWrapper(internalQuery.copy(where = Some(comparison)))
  }

  def groupBy(columns: AnyTableColumn*): OperationalQuery = {
    val newGroupingColumns: mutable.LinkedHashSet[AnyTableColumn] = mutable.LinkedHashSet(columns: _*)
    val newSelect                                                 = mergeOperationalColumns(newGroupingColumns)
    OperationalQueryWrapper(
      internalQuery.copy(selectQuery = newSelect, groupBy = internalQuery.groupBy ++ newGroupingColumns)
    )
  }

  def having(condition: Comparison): OperationalQuery = {
    val comparison = internalQuery.having.map(_.and(condition)).getOrElse(condition)
    OperationalQueryWrapper(internalQuery.copy(having = Option(comparison)))
  }

  def orderBy(columns: AnyTableColumn*): OperationalQuery =
    orderByWithDirection(columns.map(c => (c, ASC)): _*)

  def orderByWithDirection(columns: (AnyTableColumn, OrderingDirection)*): OperationalQuery = {
    val newOrderingColumns: mutable.LinkedHashSet[(_ <: AnyTableColumn, OrderingDirection)] =
      mutable.LinkedHashSet(columns: _*)
    val newSelect: SelectQuery = mergeOperationalColumns(newOrderingColumns.map(_._1))
    OperationalQueryWrapper(
      internalQuery.copy(selectQuery = newSelect, orderBy = internalQuery.orderBy ++ newOrderingColumns)
    )
  }

  def limit(limit: Option[Limit]): OperationalQuery =
    OperationalQueryWrapper(internalQuery.copy(limit = limit))

  private def mergeOperationalColumns(newOrderingColumns: mutable.LinkedHashSet[AnyTableColumn]) = {
    val selectForGroup = internalQuery.selectQuery
    val selectWithOrderColumns = selectForGroup.columns ++ newOrderingColumns.filterNot(column => {
      selectForGroup.columns.exists {
        case AliasedColumn(_, alias) => column.name == alias
        case _                       => false
      }
    })
    val newSelect = selectForGroup.copy(columns = selectWithOrderColumns)
    newSelect
  }
}

case class OperationalQueryWrapper(override val internalQuery: InternalQuery) extends OperationalQuery
