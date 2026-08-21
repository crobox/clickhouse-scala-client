package com.crobox.clickhouse.dsl.typed

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.marshalling.RowDecoder

/**
 * A query that knows the type of the rows it returns.
 *
 * A wrapper around [[OperationalQuery]] rather than a change to it: the untyped API, its ~350 functions and the whole
 * tokenizer stay exactly as they are, and the two can be mixed freely through [[transform]] and [[underlying]].
 *
 * Only the combinators that cannot change the row type are forwarded. Anything that selects -- joins, unions,
 * `distinct` -- would invalidate the decoder, so it is reached through [[transform]] deliberately rather than by
 * accident.
 */
final case class TypedQuery[R](underlying: OperationalQuery, decoder: RowDecoder[R]) {

  def from[T <: Table](table: T): TypedQuery[R]                = copy(underlying = underlying.from(table))
  def from[T <: Table](table: T, alias: String): TypedQuery[R] = copy(underlying = underlying.from(table, alias))
  def from(query: OperationalQuery): TypedQuery[R]             = copy(underlying = underlying.from(query))
  def from(query: TypedQuery[_]): TypedQuery[R]                = copy(underlying = underlying.from(query.underlying))

  def where(condition: TableColumn[Boolean]): TypedQuery[R]    = copy(underlying = underlying.where(condition))
  def prewhere(condition: TableColumn[Boolean]): TypedQuery[R] = copy(underlying = underlying.prewhere(condition))
  def having(condition: TableColumn[Boolean]): TypedQuery[R]   = copy(underlying = underlying.having(condition))

  def groupBy(columns: Column*): TypedQuery[R] = copy(underlying = underlying.groupBy(columns: _*))
  def orderBy(columns: Column*): TypedQuery[R] = copy(underlying = underlying.orderBy(columns: _*))

  def orderByWithDirection(columns: (Column, OrderingDirection)*): TypedQuery[R] =
    copy(underlying = underlying.orderByWithDirection(columns: _*))

  def limit(limit: Option[Limit]): TypedQuery[R] = copy(underlying = underlying.limit(limit))
  def limit(size: Long): TypedQuery[R]           = copy(underlying = underlying.limit(Option(Limit(size))))

  /**
   * Apply any untyped combinator this does not forward -- joins, unions, `limitBy`, `final`. The row type is asserted
   * to be unchanged, so do not use it to alter the select list.
   */
  def transform(f: OperationalQuery => OperationalQuery): TypedQuery[R] = copy(underlying = f(underlying))

  def internalQuery: InternalQuery = underlying.internalQuery
}
