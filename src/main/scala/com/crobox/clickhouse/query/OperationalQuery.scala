package com.crobox.clickhouse.query

import com.crobox.clickhouse.query.TableColumn.AnyTableColumn

import scala.collection.mutable

trait OperationalQuery extends Query with JoinableQuery {

  def where(condition: Comparison): OperationalQuery = {
    val comparison = underlying.where.map(_.and(condition)).getOrElse(condition)
    OperationalQueryWrapper(underlying.copy(where = Some(comparison)))
  }

  def groupBy(columns: AnyTableColumn*): OperationalQuery = {
    val newGroupingColumns: mutable.LinkedHashSet[AnyTableColumn] = mutable.LinkedHashSet(columns: _*)
    val newSelect                                                 = mergeOperationalColumns(newGroupingColumns)
    OperationalQueryWrapper(
      underlying.copy(selectQuery = newSelect, groupBy = underlying.groupBy ++ newGroupingColumns)
    )
  }

  def orderBy(columns: AnyTableColumn*): OperationalQuery =
    orderByWithDirection(columns.map(c => (c, ASC)): _*)

  def orderByWithDirection(columns: (AnyTableColumn, OrderingDirection)*): OperationalQuery = {
    val newOrderingColumns: mutable.LinkedHashSet[(_ <: AnyTableColumn, OrderingDirection)] =
      mutable.LinkedHashSet(columns: _*)
    val newSelect: SelectQuery = mergeOperationalColumns(newOrderingColumns.map(_._1))
    OperationalQueryWrapper(
      underlying.copy(selectQuery = newSelect, orderBy = underlying.orderBy ++ newOrderingColumns)
    )
  }

  def limit(limit: Option[Limit]): OperationalQuery =
    OperationalQueryWrapper(underlying.copy(limit = limit))

  private def mergeOperationalColumns(newOrderingColumns: mutable.LinkedHashSet[AnyTableColumn]) = {
    val selectForGroup = underlying.selectQuery
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

case class OperationalQueryWrapper(override val underlying: UnderlyingQuery) extends OperationalQuery
