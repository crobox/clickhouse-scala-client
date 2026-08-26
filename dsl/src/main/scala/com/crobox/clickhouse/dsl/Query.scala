package com.crobox.clickhouse.dsl

import scala.util.Try

trait Table {
  def database: String
  val name: String
  lazy val quoted: String =
    s"${ClickhouseStatement.quoteIdentifier(database)}.${ClickhouseStatement.quoteIdentifier(name)}"
  val columns: Seq[NativeColumn[_]]

  override def equals(obj: Any): Boolean = obj match {
    case other: Table => quoted == other.quoted
    case _            => false
  }
}

trait Query {
  val internalQuery: InternalQuery
}

/**
 * `LIMIT`, `OFFSET` and `LIMIT ... WITH TIES`.
 *
 * `size` is optional so that a bare `OFFSET n` is expressible: ClickHouse accepts one without a `LIMIT`, and folding
 * the offset into `LIMIT offset, size` -- the only form this used to render -- cannot say that.
 *
 * `withTies` extends the result past `size` with every row that ties with the last one on the `ORDER BY` key. It needs
 * an `ORDER BY` to mean anything, which is not checked here: the ordering lives on the query rather than on this.
 */
case class Limit(size: Option[Long] = Some(100), offset: Long = 0, withTies: Boolean = false) {

  // WITH TIES extends the result past `size`, so there is nothing for it to do without one, and the OFFSET-only
  // rendering has nowhere to put it -- it would be dropped silently.
  require(size.isDefined || !withTies, "WITH TIES needs a LIMIT size to extend")
}

object Limit {

  def apply(size: Long, offset: Long): Limit = Limit(Some(size), offset)

  def apply(size: Long): Limit = Limit(Some(size))
}

/** How a query is combined with the one that follows it. */
sealed abstract class SetOperationKind(val keyword: String) {

  /**
   * Whether ClickHouse binds this tighter than `UNION` and `EXCEPT`, which share one precedence level and associate
   * left to right. `INTERSECT` does: `a EXCEPT b INTERSECT c` means `a EXCEPT (b INTERSECT c)`.
   */
  def bindsTighter: Boolean = false
}

object SetOperationKind {
  case object UnionAll       extends SetOperationKind("UNION ALL")
  case object UnionDistinct  extends SetOperationKind("UNION DISTINCT")
  case object Except         extends SetOperationKind("EXCEPT")
  case object ExceptDistinct extends SetOperationKind("EXCEPT DISTINCT")

  case object Intersect extends SetOperationKind("INTERSECT") {
    override def bindsTighter: Boolean = true
  }

  case object IntersectDistinct extends SetOperationKind("INTERSECT DISTINCT") {
    override def bindsTighter: Boolean = true
  }
}

/** One `<kind> <query>` step of a set-operation chain. */
case class SetOperation(kind: SetOperationKind, query: OperationalQuery)

case class LimitBy(limit: Long, offset: Long = 0, expressions: Seq[Column])

/**
 * `ARRAY JOIN`, which unfolds an array column into one row per element.
 *
 * `left` selects `LEFT ARRAY JOIN`, which keeps rows whose array is empty instead of dropping them.
 */
case class ArrayJoinQuery(columns: Seq[Column], left: Boolean = false)

trait OrderingDirection

case object ASC extends OrderingDirection

case object DESC extends OrderingDirection

sealed case class InternalQuery(
    select: Option[SelectQuery] = None,
    from: Option[FromQuery] = None,
    prewhere: Option[TableColumn[Boolean]] = None,
    where: Option[TableColumn[Boolean]] = None,
    groupBy: Option[GroupByQuery] = None,
    having: Option[TableColumn[Boolean]] = None,
    joins: Seq[JoinQuery] = Seq.empty,
    arrayJoin: Option[ArrayJoinQuery] = None,
    orderBy: Seq[OrderingColumn] = Seq.empty,
    limit: Option[Limit] = None,
    limitBy: Option[LimitBy] = None,
    combinations: Seq[SetOperation] = Seq.empty,
    // Ordered rather than a Map: map iteration order is unspecified, which would make the emitted SQL vary between
    // runs for the same query.
    settings: Seq[(String, String)] = Seq.empty,
    // Last despite rendering first: several call sites construct InternalQuery positionally, and clause order is the
    // tokenizer's business rather than this list's.
    withEntries: Seq[WithEntry] = Seq.empty,
    // Only meaningful alongside a WITH FILL in `orderBy`; the server rejects it on a plain ORDER BY.
    interpolate: Option[Interpolate] = None,
    windows: Seq[NamedWindow] = Seq.empty,
    qualify: Option[TableColumn[Boolean]] = None
) {

  // A chain renders flat, so it only reads left to right while every step shares one precedence level. INTERSECT does
  // not: `a UNION ALL b INTERSECT c` is `a UNION ALL (b INTERSECT c)`, which is not what a Seq in that order looks
  // like. Refused rather than silently parenthesised, because either reading is a plausible thing to have meant --
  // nest the tighter part in a subquery to say which.
  // forall rather than map/distinct: this runs on every `copy`, and query building does a lot of those.
  require(
    combinations.isEmpty || combinations.forall(_.kind.bindsTighter == combinations.head.kind.bindsTighter),
    "INTERSECT binds tighter than UNION and EXCEPT, so a chain mixing them does not mean what its order says. " +
      "Put the INTERSECT in a subquery instead."
  )

  def isValid: Boolean = {
    val validGroupBy = groupBy.isEmpty && having.isEmpty || groupBy.nonEmpty

    select.isDefined && validGroupBy
  }

  def isPartial: Boolean = !isValid

  /**
   * Merge with another InternalQuery, any conflict on query parts between the 2 joins will be resolved by preferring
   * the left querypart over the right one.
   *
   * @param other
   *   The right part to merge with this InternalQuery
   * @return
   *   A merge of this and other InternalQuery
   */
  def :+>(other: InternalQuery): InternalQuery =
    // Named arguments deliberately: this was positional and passed only 10 of the 11 fields, so `unionAll` silently
    // defaulted to empty and every merge discarded unions. Naming them stops that particular slip recurring, but it
    // does NOT make a newly added field a compile error here -- an unmentioned field just takes its default and
    // disappears from every merge. `InternalQueryFieldsTest` is what catches that.
    InternalQuery(
      select = select.orElse(other.select),
      from = from.orElse(other.from),
      prewhere = prewhere.orElse(other.prewhere),
      where = where.orElse(other.where),
      groupBy = groupBy.orElse(other.groupBy),
      having = having.orElse(other.having),
      joins = if (joins.nonEmpty) joins else other.joins,
      arrayJoin = arrayJoin.orElse(other.arrayJoin),
      orderBy = if (orderBy.nonEmpty) orderBy else other.orderBy,
      limit = limit.orElse(other.limit),
      limitBy = limitBy.orElse(other.limitBy),
      combinations = if (combinations.nonEmpty) combinations else other.combinations,
      settings = if (settings.nonEmpty) settings else other.settings,
      withEntries = if (withEntries.nonEmpty) withEntries else other.withEntries,
      interpolate = interpolate.orElse(other.interpolate),
      windows = if (windows.nonEmpty) windows else other.windows,
      qualify = qualify.orElse(other.qualify)
    )

  /**
   * Right associative version of the merge (:+>) operator.
   *
   * @param other
   *   The left part to merge with this InternalQuery
   * @return
   *   A merge of this and other OperationalQuery
   */
  def <+:(other: InternalQuery): InternalQuery = :+>(other)

  /**
   * Tries to merge this InternalQuery with other
   *
   * @param other
   *   The Query parts to merge against
   * @return
   *   A Success on merge without conflict, or Failure of IllegalArgumentException otherwise.
   */
  def +(other: InternalQuery): Try[InternalQuery] = Try {
    // Checked field by field rather than reflectively over `productElement`. The reflective version matched only
    // Option/Iterable/Boolean against `Any`, so its Boolean arm was dead and any future field of another type would
    // have thrown a MatchError from inside a `require`.
    def noConflict(part: String, left: Option[_], right: Option[_]): Unit =
      require(left.isEmpty || right.isEmpty, s"Conflicting $part parts $left and $right")

    def noSeqConflict(part: String, left: Iterable[_], right: Iterable[_]): Unit =
      require(left.isEmpty || right.isEmpty, s"Conflicting $part parts $left and $right")

    noConflict("select", select, other.select)
    noConflict("from", from, other.from)
    noConflict("prewhere", prewhere, other.prewhere)
    noConflict("where", where, other.where)
    noConflict("groupBy", groupBy, other.groupBy)
    noConflict("having", having, other.having)
    noSeqConflict("joins", joins, other.joins)
    noConflict("arrayJoin", arrayJoin, other.arrayJoin)
    noSeqConflict("orderBy", orderBy, other.orderBy)
    noConflict("limit", limit, other.limit)
    noConflict("limitBy", limitBy, other.limitBy)
    noSeqConflict("combinations", combinations, other.combinations)
    noSeqConflict("settings", settings, other.settings)
    noSeqConflict("withEntries", withEntries, other.withEntries)
    noConflict("interpolate", interpolate, other.interpolate)
    noSeqConflict("windows", windows, other.windows)
    noConflict("qualify", qualify, other.qualify)

    :+>(other)
  }
}
