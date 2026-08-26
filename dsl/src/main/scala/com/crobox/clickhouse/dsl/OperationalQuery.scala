package com.crobox.clickhouse.dsl

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

  def distinctOn(distinctColumns: Column*)(columns: Column*): OperationalQuery = {
    val newSelect = Some(SelectQuery(columns, "DISTINCT ON", Some(distinctColumns)))
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
    OperationalQuery(
      internalQuery.from
        .map {
          case query: InnerFromQuery    => internalQuery.copy(from = Some(query.copy(alias = Option(alias))))
          case table: TableFromQuery[_] => internalQuery.copy(from = Some(table.copy(alias = Option(alias))))
        }
        .getOrElse(internalQuery)
    )

  def from[T <: Table](table: T): OperationalQuery =
    OperationalQuery(internalQuery.copy(from = Some(TableFromQuery(table))))

  def from[T <: Table](table: T, alias: String): OperationalQuery =
    OperationalQuery(internalQuery.copy(from = Some(TableFromQuery(table, alias = Option(alias)))))

  def from(query: OperationalQuery): OperationalQuery =
    OperationalQuery(internalQuery.copy(from = Some(InnerFromQuery(query))))

  def from(query: OperationalQuery, alias: String): OperationalQuery =
    OperationalQuery(internalQuery.copy(from = Some(InnerFromQuery(query, alias = Option(alias)))))

  def `final`: OperationalQuery = asFinal

  def asFinal: OperationalQuery =
    OperationalQuery(
      internalQuery.from
        .map {
          case _: InnerFromQuery =>
            throw new IllegalArgumentException("It's ILLEGAL to set FINAL on a (sub)query FROM query")
          case table: TableFromQuery[_] =>
            internalQuery.copy(from = Option(table.copy(finalized = true)))
        }
        .getOrElse(internalQuery)
    )

  def asFinal(`final`: Boolean): OperationalQuery = if (`final`) asFinal else this

  /**
   * `SAMPLE rate [OFFSET offset]`, where `rate` is a fraction in (0, 1] or a row count above 1.
   *
   * Only valid on a table whose engine declares a `SAMPLE BY` expression; the server rejects it otherwise. Rejected
   * here on a subquery for the same reason [[asFinal]] is -- there is no sampling key to read.
   */
  def sample(rate: Double, offset: Option[Double] = None): OperationalQuery =
    OperationalQuery(
      internalQuery.from
        .map {
          case _: InnerFromQuery =>
            throw new IllegalArgumentException("It's ILLEGAL to set SAMPLE on a (sub)query FROM query")
          case table: TableFromQuery[_] =>
            internalQuery.copy(from = Option(table.copy(sampling = Option(Sample(rate, offset)))))
        }
        .getOrElse(internalQuery)
    )

  /** `WINDOW name AS (spec)`, so several columns can share one window. */
  def window(definitions: NamedWindow*): OperationalQuery =
    OperationalQuery(internalQuery.copy(windows = internalQuery.windows ++ definitions))

  /** `QUALIFY`, which filters on window-function results the way HAVING filters on aggregates. */
  def qualify(condition: TableColumn[Boolean]): OperationalQuery = {
    val combined = internalQuery.qualify.map(_.and(condition)).getOrElse(condition)
    OperationalQuery(internalQuery.copy(qualify = Option(combined)))
  }

  /**
   * `WITH`, covering all three of the forms ClickHouse puts behind the keyword -- see [[WithEntry]]. A name introduced
   * here is referenced with `ref`, and is scoped to this query rather than to any subquery inside it.
   */
  def withCte(entries: WithEntry*): OperationalQuery =
    OperationalQuery(internalQuery.copy(withEntries = internalQuery.withEntries ++ entries))

  /**
   * `ARRAY JOIN`, unfolding each array column into one row per element. Rows with an empty array are dropped.
   *
   * Named `withArrayJoin` rather than `arrayJoin`, matching [[withRollup]] and [[withTotals]]: an inherited member
   * shadows an imported one, so an `arrayJoin` here would hide the `arrayJoin` *function* from every subclass of this
   * trait.
   */
  def withArrayJoin(columns: Column*): OperationalQuery =
    addArrayJoin(columns, left = false)

  /** `LEFT ARRAY JOIN`, which keeps a row whose array is empty rather than dropping it. */
  def withLeftArrayJoin(columns: Column*): OperationalQuery =
    addArrayJoin(columns, left = true)

  private def addArrayJoin(columns: Seq[Column], left: Boolean): OperationalQuery = {
    val existing = internalQuery.arrayJoin.map(_.columns).getOrElse(Seq.empty)
    OperationalQuery(
      internalQuery.copy(arrayJoin = Option(ArrayJoinQuery(existing ++ columns, left)))
    )
  }

  /**
   * Query-level `SETTINGS`, which unlike `QuerySettings` applies to this query alone rather than the whole HTTP
   * request, and so can differ per subquery. Values are emitted verbatim: a string setting has to arrive quoted, as in
   * `settings("log_comment" -> "'hi'")`.
   */
  def settings(setting: (String, String), settings: (String, String)*): OperationalQuery =
    OperationalQuery(
      internalQuery.copy(settings = internalQuery.settings ++ (setting +: settings))
    )

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
    orderByColumns(columns.map(OrderingColumn(_)): _*)

  def orderByWithDirection(columns: (Column, OrderingDirection)*): OperationalQuery =
    orderByColumns(columns.map(OrderingColumn.fromTuple): _*)

  /** `ORDER BY` taking the full entry, which is how a `WITH FILL` is attached to a column. */
  def orderByColumns(columns: OrderingColumn*): OperationalQuery = {
    // Appended, not deduplicated -- same as before the reshape. `DSLImprovements.+++` is where dedup lives.
    val newSelect = mergeOperationalColumns(columns.map(_.column))
    OperationalQuery(
      internalQuery.copy(select = newSelect, orderBy = internalQuery.orderBy ++ columns)
    )
  }

  /**
   * `INTERPOLATE`, carrying columns into the rows `WITH FILL` invents. Only valid alongside a `WITH FILL`; with no
   * entries it renders the bare form, which interpolates every applicable column.
   */
  def interpolate(columns: InterpolateColumn*): OperationalQuery =
    OperationalQuery(internalQuery.copy(interpolate = Option(Interpolate(columns))))

  def limit(limit: Option[Limit]): OperationalQuery =
    OperationalQuery(internalQuery.copy(limit = limit))

  def limit(size: Long): OperationalQuery = limit(Option(Limit(Option(size))))

  def limit(size: Long, offset: Long): OperationalQuery = limit(Option(Limit(Option(size), offset)))

  /** `LIMIT size WITH TIES`, which also returns every row tying with the last one on the `ORDER BY` key. */
  def limitWithTies(size: Long, offset: Long = 0): OperationalQuery =
    limit(Option(Limit(Option(size), offset, withTies = true)))

  /** `OFFSET n` with no `LIMIT`, which ClickHouse accepts on its own. */
  def offset(offset: Long): OperationalQuery =
    limit(Option(internalQuery.limit.fold(Limit(None, offset))(_.copy(offset = offset))))

  def limitBy(limit: Long, expressions: Column*): OperationalQuery =
    OperationalQuery(internalQuery.copy(limitBy = Some(LimitBy(limit, 0, expressions))))

  def limitBy(limit: Long, offset: Long, expressions: Column*): OperationalQuery =
    OperationalQuery(internalQuery.copy(limitBy = Some(LimitBy(limit, offset, expressions))))

  def unionAll(otherQuery: OperationalQuery): OperationalQuery =
    combineWith(SetOperationKind.UnionAll, otherQuery)

  /** `UNION DISTINCT`, which deduplicates across the branches where `UNION ALL` concatenates them. */
  def unionDistinct(otherQuery: OperationalQuery): OperationalQuery =
    combineWith(SetOperationKind.UnionDistinct, otherQuery)

  /**
   * `INTERSECT`, the rows present in both branches.
   *
   * Binds tighter than `UNION` and `EXCEPT`, so it cannot be chained with either -- see the `require` in
   * [[InternalQuery]].
   */
  def intersect(otherQuery: OperationalQuery): OperationalQuery =
    combineWith(SetOperationKind.Intersect, otherQuery)

  def intersectDistinct(otherQuery: OperationalQuery): OperationalQuery =
    combineWith(SetOperationKind.IntersectDistinct, otherQuery)

  /** `EXCEPT`, the rows of this query that the other does not have. */
  def except(otherQuery: OperationalQuery): OperationalQuery =
    combineWith(SetOperationKind.Except, otherQuery)

  def exceptDistinct(otherQuery: OperationalQuery): OperationalQuery =
    combineWith(SetOperationKind.ExceptDistinct, otherQuery)

  private def combineWith(kind: SetOperationKind, otherQuery: OperationalQuery): OperationalQuery = {
    require(
      internalQuery.select.isDefined && otherQuery.internalQuery.select.isDefined,
      s"Trying to apply ${kind.keyword} on non SELECT queries."
    )
    require(
      otherQuery.internalQuery.select.get.columns.size == internalQuery.select.get.columns.size,
      s"SELECT queries needs to have the same number of columns to perform ${kind.keyword}."
    )

    OperationalQuery(
      internalQuery.copy(combinations = internalQuery.combinations :+ SetOperation(kind, otherQuery))
    )
  }

  /**
   * The name a column publishes to the enclosing scope, where it has one.
   *
   * `Column.name` is not that. An `ExpressionColumn` is constructed with its argument's name, so `sum(x).name` is `x`
   * -- which is why de-duplicating on `name` left the grouping column out of `select(sum(x)).groupBy(x)`. Only these
   * three name their own output, and only they can stand in for one another.
   */
  private def publishedName(column: Column): Option[String] = column match {
    case c: AliasedColumn[_] => Option(c.alias)
    case c: NativeColumn[_]  => Option(c.name)
    case c: RefColumn[_]     => Option(c.ref)
    case _                   => None
  }

  /**
   * Adds the `GROUP BY` / `ORDER BY` columns that the projection does not already produce.
   *
   * "Already produce" is two things, and matching only one of them has broken in both directions: by name alone drops a
   * grouping column that an aggregate over it appears to shadow, and by value alone appends a second `value` for
   * `ORDER BY value` over a `... AS value`, silently widening every row.
   */
  private def mergeOperationalColumns(newOrderingColumns: Seq[Column]): Option[SelectQuery] = {
    val selectForGroup = internalQuery.select

    val selectForGroupCols = selectForGroup.toSeq.flatMap(_.columns)

    val filteredSelectAll = if (selectForGroupCols.contains(all)) {
      // Only keep aliased, we already select all cols
      newOrderingColumns.collect { case c: AliasedColumn[_] => c }
    } else {
      newOrderingColumns
    }

    val published = selectForGroupCols.flatMap(publishedName).toSet

    val filteredDuplicates = filteredSelectAll.filterNot { candidate =>
      selectForGroupCols.contains(candidate) || publishedName(candidate).exists(published.contains)
    }

    val selectWithOrderColumns = selectForGroupCols ++ filteredDuplicates

    val newSelect = selectForGroup.map(sq => sq.copy(columns = selectWithOrderColumns))
    newSelect
  }

  /**
   * Appends a join. Several joins at one level render as a chain, where each one's `ON` addresses the *first* table --
   * the left alias stays `L1` throughout while the right aliases count up `R1`, `R2`, ... That covers a fact table
   * joined to several dimensions; anything keyed off an earlier join's right-hand side has to nest in a subquery, since
   * a [[JoinCondition]] names columns rather than the table they belong to.
   */
  def join[TargetTable <: Table](
      joinType: JoinQuery.JoinType,
      query: OperationalQuery,
      global: Boolean
  ): OperationalQuery = addJoin(JoinQuery(joinType, InnerFromQuery(query), global = global))

  def join[TargetTable <: Table](joinType: JoinQuery.JoinType, table: TargetTable, global: Boolean): OperationalQuery =
    addJoin(JoinQuery(joinType, TableFromQuery(table), global = global))

  private def addJoin(join: JoinQuery): OperationalQuery =
    OperationalQuery(internalQuery.copy(joins = internalQuery.joins :+ join))

  /**
   * Rewrites the join most recently added, which is what `using` and `on` attach to.
   *
   * Applies to the last rather than the only one so that `.join(a).using(k).join(b).using(k)` reads as it looks.
   */
  private def mapLastJoin(f: JoinQuery => JoinQuery): OperationalQuery = {
    require(internalQuery.joins.nonEmpty, "No join to attach USING or ON to")
    OperationalQuery(internalQuery.copy(joins = internalQuery.joins.init :+ f(internalQuery.joins.last)))
  }

  def join[TargetTable <: Table](joinType: JoinQuery.JoinType, query: OperationalQuery): OperationalQuery =
    join(joinType = joinType, query = query, global = false)

  def join[TargetTable <: Table](joinType: JoinQuery.JoinType, table: TargetTable): OperationalQuery =
    join(joinType = joinType, table = table, global = false)

  def globalJoin[TargetTable <: Table](joinType: JoinQuery.JoinType, query: OperationalQuery): OperationalQuery =
    join(joinType = joinType, query = query, global = true)

  def globalJoin[TargetTable <: Table](joinType: JoinQuery.JoinType, table: TargetTable): OperationalQuery =
    join(joinType = joinType, table = table, global = true)

  def using(
      column: Column,
      columns: Column*
  ): OperationalQuery = mapLastJoin(_.copy(`using` = (column +: columns).distinct))

  def on(columns: Column*): OperationalQuery =
    mapLastJoin(_.copy(on = columns.map(JoinCondition(_))))

  def on(condition: JoinCondition, conditions: JoinCondition*): OperationalQuery =
    mapLastJoin(_.copy(on = condition +: conditions))

  def on(condition: (Column, String, Column), conditions: (Column, String, Column)*): OperationalQuery =
    mapLastJoin(_.copy(on = JoinCondition(condition) +: conditions.map(JoinCondition(_))))

  /**
   * Merge with another OperationalQuery, any conflict on query parts between the 2 joins will be resolved by preferring
   * the left querypart over the right one.
   *
   * @param other
   *   The right part to merge with this OperationalQuery
   * @return
   *   A merge of this and other OperationalQuery
   */
  def :+>(other: OperationalQuery): OperationalQuery =
    OperationalQuery(this.internalQuery :+> other.internalQuery)

  /**
   * Right associative version of the merge (:+>) operator.
   *
   * @param other
   *   The left part to merge with this OperationalQuery
   * @return
   *   A merge of this and other OperationalQuery
   */

  def <+:(other: OperationalQuery): OperationalQuery =
    OperationalQuery(this.internalQuery :+> other.internalQuery)

  /**
   * Tries to merge this OperationalQuery with other
   *
   * @param other
   *   The Query parts to merge against
   * @return
   *   A Success on merge without conflict, or Failure of IllegalArgumentException otherwise.
   */
  def +(other: OperationalQuery): Try[OperationalQuery] =
    (this.internalQuery + other.internalQuery).map(OperationalQuery.apply)

  def +(other: Try[OperationalQuery]): Try[OperationalQuery] =
    other
      .flatMap(o => this.internalQuery + o.internalQuery)
      .map(OperationalQuery.apply)
}
