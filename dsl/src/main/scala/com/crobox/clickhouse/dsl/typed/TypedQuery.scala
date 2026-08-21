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
 *
 * [[groupBy]] and [[orderBy]] are reimplemented rather than forwarded. The untyped versions run the grouping and
 * ordering columns through `mergeOperationalColumns`, which appends any of them that the select list does not already
 * contain -- harmless when decoding is keyed by name and extra fields are ignored, but here it would widen the row
 * behind the decoder's back. `SELECT count() FROM t GROUP BY x` is valid SQL and is what the caller asked for, so the
 * clause is added and the select list left alone.
 */
final case class TypedQuery[R](underlying: OperationalQuery, decoder: RowDecoder[R]) {

  def from[T <: Table](table: T): TypedQuery[R]                = copy(underlying = underlying.from(table))
  def from[T <: Table](table: T, alias: String): TypedQuery[R] = copy(underlying = underlying.from(table, alias))
  def from(query: OperationalQuery): TypedQuery[R]             = copy(underlying = underlying.from(query))
  def from(query: TypedQuery[_]): TypedQuery[R]                = copy(underlying = underlying.from(query.underlying))

  def where(condition: TableColumn[Boolean]): TypedQuery[R]    = copy(underlying = underlying.where(condition))
  def prewhere(condition: TableColumn[Boolean]): TypedQuery[R] = copy(underlying = underlying.prewhere(condition))
  def having(condition: TableColumn[Boolean]): TypedQuery[R]   = copy(underlying = underlying.having(condition))

  def groupBy(columns: Column*): TypedQuery[R] = withInternalQuery { query =>
    val existing = query.groupBy.getOrElse(GroupByQuery())
    query.copy(groupBy = Some(existing.copy(usingColumns = existing.usingColumns ++ columns)))
  }

  def orderBy(columns: Column*): TypedQuery[R] = orderByWithDirection(columns.map(column => (column, ASC)): _*)

  def orderByWithDirection(columns: (Column, OrderingDirection)*): TypedQuery[R] =
    withInternalQuery(query => query.copy(orderBy = query.orderBy ++ columns))

  def limit(limit: Option[Limit]): TypedQuery[R] = copy(underlying = underlying.limit(limit))
  def limit(size: Long): TypedQuery[R]           = copy(underlying = underlying.limit(Option(Limit(size))))

  /**
   * Apply any untyped combinator this does not forward -- joins, unions, `limitBy`, `final`. The row type is asserted
   * to be unchanged, so do not use it to alter the select list.
   */
  def transform(f: OperationalQuery => OperationalQuery): TypedQuery[R] = copy(underlying = f(underlying))

  def internalQuery: InternalQuery = underlying.internalQuery

  private def withInternalQuery(f: InternalQuery => InternalQuery): TypedQuery[R] =
    copy(underlying = OperationalQuery(f(underlying.internalQuery)))
}
