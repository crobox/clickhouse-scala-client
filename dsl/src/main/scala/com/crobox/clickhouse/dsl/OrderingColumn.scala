package com.crobox.clickhouse.dsl

import scala.language.implicitConversions

/**
 * `WITH FILL` on one `ORDER BY` column: rows for the gaps in an otherwise sparse ordering.
 *
 * Bounds are [[Column]]s rather than numbers because the common use is filling a time series, where they are dates --
 * `Option(toDate("2020-01-01"))`. Use `const(...)` for a plain number.
 *
 * https://clickhouse.com/docs/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier
 */
case class WithFill(
    from: Option[Column] = None,
    to: Option[Column] = None,
    step: Option[Column] = None,
    staleness: Option[Column] = None
) {

  // The server rejects this combination with INVALID_WITH_FILL_EXPRESSION; failing here says so in Scala terms.
  require(
    !(from.isDefined && staleness.isDefined),
    "WITH FILL STALENESS cannot be combined with WITH FILL FROM"
  )
}

/** `NULLS FIRST` / `NULLS LAST`, which decides where NULLs land independently of the direction. */
sealed abstract class NullsOrder(val keyword: String)

object NullsOrder {
  case object NullsFirst extends NullsOrder("NULLS FIRST")
  case object NullsLast  extends NullsOrder("NULLS LAST")
}

/**
 * One `ORDER BY` entry.
 *
 * Was a `(Column, OrderingDirection)` tuple, which had nowhere to put `WITH FILL`. The implicit conversions below keep
 * the tuple and bare-column forms working at call sites.
 *
 * `nulls` and `collate` come after `fill` in the parameter list but before it in the emitted SQL. Field order is kept
 * as it was so the existing positional `OrderingColumn(col, DESC, Option(WithFill()))` call sites still compile;
 * ClickHouse's grammar fixes the render order at `expr [ASC|DESC] [NULLS ...] [COLLATE ...] [WITH FILL ...]` and
 * rejects any other -- `COLLATE 'en' NULLS LAST` and `WITH FILL ... NULLS LAST` are both syntax errors.
 */
case class OrderingColumn(
    column: Column,
    direction: OrderingDirection = ASC,
    fill: Option[WithFill] = None,
    nulls: Option[NullsOrder] = None,
    collate: Option[String] = None
)

object OrderingColumn {

  implicit def fromColumn(column: Column): OrderingColumn = OrderingColumn(column)

  implicit def fromTuple(pair: (Column, OrderingDirection)): OrderingColumn = OrderingColumn(pair._1, pair._2)
}

/**
 * One `INTERPOLATE` entry: a column to carry into the rows `WITH FILL` invents, optionally through an expression.
 *
 * `INTERPOLATE (tag)` repeats the previous value; `INTERPOLATE (tag AS concat(tag, 'x'))` derives it.
 */
case class InterpolateColumn(column: Column, expression: Option[Column] = None)

/**
 * `INTERPOLATE [(expr_list)]`, valid only alongside `WITH FILL` -- the server rejects it on a plain `ORDER BY`.
 *
 * An empty [[columns]] renders the bare form, which interpolates every applicable column.
 */
case class Interpolate(columns: Seq[InterpolateColumn] = Seq.empty)
