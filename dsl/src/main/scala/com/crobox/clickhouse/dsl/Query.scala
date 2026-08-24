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

case class Limit(size: Long = 100, offset: Long = 0)

case class LimitBy(limit: Long, offset: Long = 0, expressions: Seq[Column])

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
    join: Option[JoinQuery] = None,
    orderBy: Seq[(Column, OrderingDirection)] = Seq.empty,
    limit: Option[Limit] = None,
    limitBy: Option[LimitBy] = None,
    unionAll: Seq[OperationalQuery] = Seq.empty
) {

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
      join = join.orElse(other.join),
      orderBy = if (orderBy.nonEmpty) orderBy else other.orderBy,
      limit = limit.orElse(other.limit),
      limitBy = limitBy.orElse(other.limitBy),
      unionAll = if (unionAll.nonEmpty) unionAll else other.unionAll
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
    noConflict("join", join, other.join)
    noSeqConflict("orderBy", orderBy, other.orderBy)
    noConflict("limit", limit, other.limit)
    noConflict("limitBy", limitBy, other.limitBy)
    noSeqConflict("unionAll", unionAll, other.unionAll)

    :+>(other)
  }
}
