package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.misc.RandomStringGenerator

import scala.util.Try

object OperationalQuery {

  def apply(query: InternalQuery): OperationalQuery = new OperationalQuery {
    override val internalQuery: InternalQuery = query
  }

}

trait OperationalQuery extends Query {

  def select(columns: Column*): OperationalQuery = {
    val newSelect = Some(SelectQuery(Seq(columns: _*)))
    OperationalQuery(internalQuery.copy(select = newSelect))
  }

  def distinct(columns: Column*): OperationalQuery = {
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

  def as(alias: String): OperationalQuery =
    OperationalQuery(internalQuery.copy(as = Some(alias)))

  def from[T <: Table](table: T): OperationalQuery = {
    val from = TableFromQuery(table)
    OperationalQuery(internalQuery.copy(from = Some(from)))
  }

  def from(query: OperationalQuery, alias: Option[String] = None): OperationalQuery = {
    val from = InnerFromQuery(query, alias = alias.orElse(Option(RandomStringGenerator.random())))
    OperationalQuery(internalQuery.copy(from = Some(from)))
  }

  def asFinal: OperationalQuery =
    OperationalQuery(internalQuery.copy(as = Option("FINAL")))

  def groupBy(columns: Column*): OperationalQuery = {
    val internalGroupBy = internalQuery.groupBy.getOrElse(GroupByQuery())
    val newGroupBy      = Some(internalGroupBy.copy(usingColumns = internalGroupBy.usingColumns ++ columns))
    val newSelect       = mergeOperationalColumns(columns)
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

  def orderBy(columns: Column*): OperationalQuery =
    orderByWithDirection(columns.map(c => (c, ASC)): _*)

  def orderByWithDirection(columns: (Column, OrderingDirection)*): OperationalQuery = {
    val newOrderingColumns: Seq[(Column, OrderingDirection)] =
      Seq(columns: _*)
    val newSelect = mergeOperationalColumns(columns.map(_._1))
    OperationalQuery(
      internalQuery.copy(select = newSelect, orderBy = internalQuery.orderBy ++ newOrderingColumns)
    )
  }

  def limit(limit: Option[Limit]): OperationalQuery =
    OperationalQuery(internalQuery.copy(limit = limit))

  def unionAll(otherQuery: OperationalQuery): OperationalQuery = {
    require(internalQuery.select.isDefined && otherQuery.internalQuery.select.isDefined,
            "Trying to apply UNION ALL on non SELECT queries.")
    require(
      otherQuery.internalQuery.select.get.columns.size == internalQuery.select.get.columns.size,
      "SELECT queries needs to have the same number of columns to perform UNION ALL."
    )

    OperationalQuery(internalQuery.copy(unionAll = internalQuery.unionAll :+ otherQuery))
  }

  private def mergeOperationalColumns(newOrderingColumns: Seq[Column]): Option[SelectQuery] = {
    val selectForGroup = internalQuery.select

    val selectForGroupCols = selectForGroup.toSeq.flatMap(_.columns)

    val filteredSelectAll = if (selectForGroupCols.contains(all())) {
      //Only keep aliased, we already select all cols
      newOrderingColumns.collect { case c: AliasedColumn[_] => c }
    } else {
      newOrderingColumns
    }

    val filteredDuplicates = filteredSelectAll.filterNot(column => {
      selectForGroupCols.exists {
        case c: Column => column.name == c.name
        case _         => false
      }
    })

    val selectWithOrderColumns = selectForGroupCols ++ filteredDuplicates

    val newSelect = selectForGroup.map(sq => sq.copy(columns = selectWithOrderColumns))
    newSelect
  }

  def join[TargetTable <: Table](joinType: JoinQuery.JoinType,
                                 query: OperationalQuery,
                                 alias: Option[String]): OperationalQuery =
    OperationalQuery(
      internalQuery.copy(
        join =
          Some(JoinQuery(joinType, InnerFromQuery(query, alias = alias.orElse(Option(RandomStringGenerator.random())))))
      )
    )

  def join[TargetTable <: Table](joinType: JoinQuery.JoinType, table: TargetTable): OperationalQuery =
    OperationalQuery(internalQuery.copy(join = Some(JoinQuery(joinType, TableFromQuery(table)))))

  private val AsOfJoinTypes = Set(JoinQuery.AsOfJoin, JoinQuery.AsOfLeftJoin)
  private val AsOfOperators = Set(">", ">=", "<", "<=")

  /**
   *
   * @param joinType
   * @param query
   * @param alias
   * @param matchConditions Are used by AsOfJoin queries. Must be provided as tuples of (Column, Operator), where operator
   *                        must be one of the following: >, >=, <, <=
   * @tparam TargetTable
   * @return
   */
  def asOfJoin[TargetTable <: Table](joinType: JoinQuery.JoinType,
                                     query: OperationalQuery,
                                     alias: Option[String],
                                     matchConditions: (Column, String)*): OperationalQuery = {
    assert(AsOfJoinTypes.exists(_ == joinType), s"Join Type must be one of: $AsOfJoinTypes")
    assert(matchConditions.nonEmpty, s"No matchConditions provided for joinType: $joinType")
    matchConditions.foreach(
      tuple =>
        assert(AsOfOperators.contains(tuple._2),
               s"matchCondition ($tuple) must contain supported operator: $AsOfOperators")
    )
    OperationalQuery(
      internalQuery.copy(
        join = Some(
          JoinQuery(joinType, InnerFromQuery(query, alias), matchConditions = Seq(matchConditions: _*))
        )
      )
    )
  }

  /**
   *
   * @param joinType
   * @param table
   * @param alias
   * @param matchConditions Are used by AsOfJoin queries. Must be provided as tuples of (Column, Operator), where operator
   *                        must be one of the following: >, >=, <, <=
   * @tparam TargetTable
   * @return
   */
  def asOfJoin[TargetTable <: Table](joinType: JoinQuery.JoinType,
                                     table: TargetTable,
                                     matchConditions: (Column, String)*): OperationalQuery = {
    assert(AsOfJoinTypes.exists(_ == joinType), s"Join Type must be one of: $AsOfJoinTypes")
    assert(matchConditions.nonEmpty, s"No matchConditions provided for joinType: $joinType")
    matchConditions.foreach(
      tuple =>
        assert(AsOfOperators.contains(tuple._2),
               s"matchCondition ($tuple) must contain supported operator: $AsOfOperators")
    )
    OperationalQuery(
      internalQuery.copy(
        join = Some(
          JoinQuery(joinType, TableFromQuery(table), matchConditions = Seq(matchConditions: _*))
        )
      )
    )
  }

  def globalJoin[TargetTable <: Table](joinType: JoinQuery.JoinType,
                                       query: OperationalQuery,
                                       alias: Option[String]): OperationalQuery =
    OperationalQuery(internalQuery.copy(join = Some(JoinQuery(joinType, InnerFromQuery(query, alias), global = true))))

  def globalJoin[TargetTable <: Table](joinType: JoinQuery.JoinType, table: TargetTable): OperationalQuery =
    OperationalQuery(internalQuery.copy(join = Some(JoinQuery(joinType, TableFromQuery(table), global = true))))

  @deprecated("Please use join(JoinQuery.AllInnerJoin)")
  def allInnerJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    join(JoinQuery.AllInnerJoin, query, alias)

  @deprecated("Please use join(JoinQuery.AllLeftJoin)")
  def allLeftJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    join(JoinQuery.AllLeftJoin, query, alias)

  @deprecated("Please use join(JoinQuery.AllRightJoin)")
  def allRightJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    join(JoinQuery.AllRightJoin, query, alias)

  @deprecated("Please use join(JoinQuery.AllInnerJoin)", "Clickhouse v20")
  def anyInnerJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    join(JoinQuery.AnyInnerJoin, query, alias)

  @deprecated("Please use join(JoinQuery.AnyLeftJoin)")
  def anyLeftJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    join(JoinQuery.AnyLeftJoin, query, alias)

  @deprecated("Please use join(JoinQuery.AllRightJoin)", "Clickhouse v20")
  def anyRightJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    join(JoinQuery.AnyRightJoin, query, alias)

  @deprecated("Please use globalJoin(JoinQuery.AllInnerJoin)")
  def globalAllInnerJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    globalJoin(JoinQuery.AllInnerJoin, query, alias)

  @deprecated("Please use globalJoin(JoinQuery.AllLeftJoin)")
  def globalAllLeftJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    globalJoin(JoinQuery.AllLeftJoin, query, alias)

  @deprecated("Please use globalJoin(JoinQuery.AllRightJoin)")
  def globalAllRightJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    globalJoin(JoinQuery.AllRightJoin, query, alias)

  @deprecated("Please use globalJoin(JoinQuery.AllInnerJoin)", "Clickhouse v20")
  def globalAnyInnerJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    globalJoin(JoinQuery.AnyInnerJoin, query, alias)

  @deprecated("Please use globalJoin(JoinQuery.AnyLeftJoin)")
  def globalAnyLeftJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    globalJoin(JoinQuery.AnyLeftJoin, query, alias)

  @deprecated("Please use globalJoin(JoinQuery.AllRightJoin)", "Clickhouse v20")
  def globalAnyRightJoin(query: OperationalQuery, alias: Option[String] = None): OperationalQuery =
    globalJoin(JoinQuery.AnyRightJoin, query, alias)

  def using(
      column: Column,
      columns: Column*
  ): OperationalQuery = {
    require(internalQuery.join.isDefined)

    val newUsing = (column +: columns).distinct
    val newJoin  = this.internalQuery.join.get.copy(joinKeys = newUsing)

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
