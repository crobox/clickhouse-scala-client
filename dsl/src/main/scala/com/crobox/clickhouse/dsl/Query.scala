package com.crobox.clickhouse.dsl

import scala.util.Try

trait Table {
  lazy val database: String = ""
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

trait OrderingDirection

case object ASC extends OrderingDirection

case object DESC extends OrderingDirection

sealed case class InternalQuery(select: Option[SelectQuery] = None,
                                from: Option[FromQuery] = None,
                                prewhere: Option[TableColumn[Boolean]] = None,
                                where: Option[TableColumn[Boolean]] = None,
                                groupBy: Option[GroupByQuery] = None,
                                having: Option[TableColumn[Boolean]] = None,
                                join: Option[JoinQuery] = None,
                                orderBy: Seq[(Column, OrderingDirection)] = Seq.empty,
                                limit: Option[Limit] = None,
                                unionAll: Seq[OperationalQuery] = Seq.empty) {

  def isValid: Boolean = {
    val validGroupBy = groupBy.isEmpty && having.isEmpty || groupBy.nonEmpty

    select.isDefined && validGroupBy
  }

  def isPartial: Boolean = !isValid

  /**
   * Merge with another InternalQuery, any conflict on query parts between the 2 joins will be resolved by
   * preferring the left querypart over the right one.
   *
   * @param other The right part to merge with this InternalQuery
   * @return A merge of this and other InternalQuery
   */
  def :+>(other: InternalQuery): InternalQuery =
    InternalQuery(
      select.orElse(other.select),
      from.orElse(other.from),
      prewhere.orElse(other.prewhere),
      where.orElse(other.where),
      groupBy.orElse(other.groupBy),
      having.orElse(other.having),
      join.orElse(other.join),
      if (orderBy.nonEmpty) orderBy else other.orderBy,
      limit.orElse(other.limit)
    )

  /**
   * Right associative version of the merge (:+>) operator.
   *
   * @param other The left part to merge with this InternalQuery
   * @return A merge of this and other OperationalQuery
   */
  def <+:(other: InternalQuery): InternalQuery = :+>(other)

  /**
   * Tries to merge this InternalQuery with other
   *
   * @param other The Query parts to merge against
   * @return A Success on merge without conflict, or Failure of IllegalArgumentException otherwise.
   */
  def +(other: InternalQuery): Try[InternalQuery] = Try {
    (0 until productArity).foreach(id => {
      require(
        (productElement(id), other.productElement(id)) match {
          case (ts: Option[_], tt: Option[_]) =>
            ts.isEmpty || tt.isEmpty
          case (ts: Iterable[_], tt: Iterable[_]) =>
            ts.isEmpty || tt.isEmpty
          case (ts: Boolean, tt: Boolean) =>
            true
        },
        s"Conflicting parts ${productElement(id)} and ${other.productElement(id)}"
      )
    })
    :+>(other)
  }
}
