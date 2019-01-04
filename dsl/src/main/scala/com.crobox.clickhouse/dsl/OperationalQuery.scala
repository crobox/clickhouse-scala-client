package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

import scala.util.Try

object OperationalQuery {

  def apply(query: InternalQuery): OperationalQuery = new OperationalQuery {
    override val internalQuery: InternalQuery = query
  }

}

trait OperationalQuery extends Query {

  def select(columns: AnyTableColumn*): OperationalQuery = {
    val newSelect = Some(SelectQuery(Seq(columns: _*)))
    OperationalQuery(internalQuery.copy(select = newSelect))
  }

  def distinct(columns: AnyTableColumn*): OperationalQuery = {
    val newSelect = Some(SelectQuery(Seq(columns: _*), "DISTINCT"))
    OperationalQuery(internalQuery.copy(select = newSelect))
  }

  def prewhere(condition: TableColumn[Boolean]): OperationalQuery = {
    val comparison = internalQuery.prewhere.map(_.and(condition)).getOrElse(condition)
    OperationalQuery(internalQuery.copy(prewhere = Some(comparison)))
  }

  def where(condition: TableColumn[Boolean]): OperationalQuery = {
    val comparison = internalQuery.where.map(_.and(condition)).getOrElse(condition)
    OperationalQuery(internalQuery.copy(where = Some(comparison)))
  }

  def from[T <: Table](table: T, altDb: Option[String] = None): OperationalQuery = {
    val from = TableFromQuery(table, altDb)
    OperationalQuery(internalQuery.copy(from = Some(from)))
  }

  def from(query: OperationalQuery): OperationalQuery = {
    val from = InnerFromQuery(query)
    OperationalQuery(internalQuery.copy(from = Some(from)))
  }

  def asFinal: OperationalQuery = {
    OperationalQuery(internalQuery.copy(asFinal = true))
  }

  def groupBy(columns: AnyTableColumn*): OperationalQuery = {
    val internalGroupBy = internalQuery.groupBy.getOrElse(GroupByQuery())
    val newGroupBy = Some(internalGroupBy.copy(usingColumns = internalGroupBy.usingColumns ++ columns))
    val newSelect = mergeOperationalColumns(columns)
    OperationalQuery(
      internalQuery.copy(select = newSelect, groupBy = newGroupBy)
    )
  }

  def withRollup: OperationalQuery = {
    val newGroupBy = internalQuery.groupBy.getOrElse(GroupByQuery()).copy(mode = Some(GroupByQuery.WithRollup))
    OperationalQuery(
      internalQuery.copy(groupBy = Some(newGroupBy))
    )
  }

  def withCube: OperationalQuery = {
    val newGroupBy = internalQuery.groupBy.getOrElse(GroupByQuery()).copy(mode = Some(GroupByQuery.WithCube))
    OperationalQuery(
      internalQuery.copy(groupBy = Some(newGroupBy))
    )
  }

  def withTotals: OperationalQuery = {
    val newGroupBy = internalQuery.groupBy.getOrElse(GroupByQuery()).copy(withTotals = true)
    OperationalQuery(
      internalQuery.copy(groupBy = Some(newGroupBy))
    )
  }

  def having(condition: TableColumn[Boolean]): OperationalQuery = {
    val comparison = internalQuery.having.map(_.and(condition)).getOrElse(condition)
    OperationalQuery(internalQuery.copy(having = Option(comparison)))
  }

  def orderBy(columns: AnyTableColumn*): OperationalQuery =
    orderByWithDirection(columns.map(c => (c, ASC)): _*)

  def orderByWithDirection(columns: (AnyTableColumn, OrderingDirection)*): OperationalQuery = {
    val newOrderingColumns: Seq[(_ <: AnyTableColumn, OrderingDirection)] =
      Seq(columns: _*)
    val newSelect = mergeOperationalColumns(columns.map(_._1))
    OperationalQuery(
      internalQuery.copy(select = newSelect, orderBy = internalQuery.orderBy ++ newOrderingColumns)
    )
  }

  def limit(limit: Option[Limit]): OperationalQuery =
    OperationalQuery(internalQuery.copy(limit = limit))

  def unionAll(otherQuery : OperationalQuery): OperationalQuery = {
    require(internalQuery.select.isDefined && otherQuery.internalQuery.select.isDefined, "Trying to apply UNION ALL on non SELECT queries.")
    require(otherQuery.internalQuery.select.get.columns.size == internalQuery.select.get.columns.size,
      "SELECT queries needs to have the same number of columns to perform UNION ALL."
    )

    OperationalQuery(internalQuery.copy(unionAll = internalQuery.unionAll :+ otherQuery))
  }

  private def mergeOperationalColumns(newOrderingColumns: Seq[AnyTableColumn]): Option[SelectQuery] = {
    val selectForGroup     = internalQuery.select

    val selectForGroupCols = selectForGroup.toSeq.flatMap(_.columns)

    val filteredSelectAll = if (selectForGroupCols.contains(all())) {
      //Only keep aliased, we already select all cols
      newOrderingColumns.collect{ case c:AliasedColumn[_] => c}
    }else{
      newOrderingColumns
    }

    val filteredDuplicates = filteredSelectAll.filterNot(column => {
      selectForGroupCols.exists {
        case c: Column => column.name == c.name
        case _                       => false
      }
    })

    val selectWithOrderColumns = selectForGroupCols ++ filteredDuplicates

    val newSelect = selectForGroup.map(sq => sq.copy(columns = selectWithOrderColumns))
    newSelect
  }

  def allInnerJoin(query: OperationalQuery): OperationalQuery = {
    val newJoin = JoinQuery(JoinQuery.AllInnerJoin, InnerFromQuery(query))
    OperationalQuery(internalQuery.copy(join = Some(newJoin)))
  }

  def allLeftJoin(query: OperationalQuery): OperationalQuery = {
    val newJoin = JoinQuery(JoinQuery.AllLeftJoin, InnerFromQuery(query))
    OperationalQuery(internalQuery.copy(join = Some(newJoin)))
  }

  def anyLeftJoin(query: OperationalQuery): OperationalQuery = {
    val newJoin = JoinQuery(JoinQuery.AnyLeftJoin, InnerFromQuery(query))
    OperationalQuery(internalQuery.copy(join = Some(newJoin)))
  }

  def anyInnerJoin(query: OperationalQuery): OperationalQuery = {
    val newJoin = JoinQuery(JoinQuery.AnyInnerJoin, InnerFromQuery(query))
    OperationalQuery(internalQuery.copy(join = Some(newJoin)))
  }

  def join[TargetTable <: Table](`type`: JoinQuery.JoinType, query: OperationalQuery): OperationalQuery = {
    val newJoin = JoinQuery(`type`, InnerFromQuery(query))
    OperationalQuery(internalQuery.copy(join = Some(newJoin)))
  }

  def join[TargetTable <: Table](`type`: JoinQuery.JoinType, table: TargetTable): OperationalQuery = {
    val newJoin = JoinQuery(`type`, TableFromQuery(table))
    OperationalQuery(internalQuery.copy(join = Some(newJoin)))
  }

  def using(
    column: AnyTableColumn,
    columns: AnyTableColumn*
  ): OperationalQuery = {
    require(internalQuery.join.isDefined)

    val newUsing = (columns :+ column).toSet

    val newJoin = this.internalQuery.join.get.copy(usingColumns = newUsing)

    OperationalQuery(internalQuery.copy(join = Some(newJoin)))
  }

  /**
    * Merge with another OperationalQuery, any conflict on query parts between the 2 joins will be resolved by
    * preferring the left querypart over the right one.
    *
    * @param other The right part to merge with this OperationalQuery
    * @return A merge of this and other OperationalQuery
    */
  def :+>(other: OperationalQuery): OperationalQuery =
    OperationalQuery(this.internalQuery :+> other.internalQuery)

  /**
    * Right associative version of the merge (:+>) operator.
    *
    * @param other The left part to merge with this OperationalQuery
    * @return A merge of this and other OperationalQuery
    */

  def <+:(other: OperationalQuery): OperationalQuery =
    OperationalQuery(this.internalQuery :+> other.internalQuery)

  /**
    * Tries to merge this OperationalQuery with other
    *
    * @param other The Query parts to merge against
    * @return A Success on merge without conflict, or Failure of IllegalArgumentException otherwise.
    */
  def +(other: OperationalQuery): Try[OperationalQuery] =
    (this.internalQuery + other.internalQuery).map(OperationalQuery.apply)

  def +(other: Try[OperationalQuery]): Try[OperationalQuery] =
    other
      .flatMap(o => this.internalQuery + o.internalQuery)
      .map(OperationalQuery.apply)
}
