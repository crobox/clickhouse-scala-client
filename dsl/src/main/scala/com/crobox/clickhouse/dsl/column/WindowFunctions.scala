package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._

trait WindowFunctions { self: Magnets =>

  /**
   * A function ClickHouse accepts only with `OVER`. On its own each of these fails with "The function 'row_number' can
   * only be used as a window function, not as an aggregate function".
   *
   * Deliberately not a [[com.crobox.clickhouse.dsl.Column]]: `over` is the only way to reach one, so
   * `select(rowNumber())` is a compile error rather than a server error. Same reasoning that puts `over` on
   * [[AggregationFunctions.AggregateFunction]] rather than on `TableColumn` -- there the restriction rules out
   * `v OVER ()`, here it rules out a window function without its window.
   */
  final class WindowOnlyFunction[+V] private[column] (private[dsl] val call: WindowOnlyFunctionCol[V]) {

    /** `OVER (spec)`. */
    def over(spec: WindowSpec): WindowFunction[V] = WindowFunction(call, WindowRef.Inline(spec))

    /** `OVER name`, referring to a definition in the query's `WINDOW` clause. */
    def over(window: NamedWindow): WindowFunction[V] = WindowFunction(call, WindowRef.Named(window.name))

    /** `OVER ()`, an unpartitioned, unordered window over the whole result. */
    def over(): WindowFunction[V] = over(WindowSpec())
  }

  abstract class WindowOnlyFunctionCol[+V](targetColumn: Column) extends ExpressionColumn[V](targetColumn)

  case class RowNumber()   extends WindowOnlyFunctionCol[Long](EmptyColumn)
  case class Rank()        extends WindowOnlyFunctionCol[Long](EmptyColumn)
  case class DenseRank()   extends WindowOnlyFunctionCol[Long](EmptyColumn)
  case class PercentRank() extends WindowOnlyFunctionCol[Double](EmptyColumn)

  case class Ntile(buckets: Long) extends WindowOnlyFunctionCol[Long](EmptyColumn) {

    // The server rejects both a non-constant and a non-positive bucket count with BAD_ARGUMENTS; a constant is all this
    // signature accepts, so only the range is left to check.
    require(buckets > 0, s"ntile needs a positive bucket count, got $buckets")
  }

  /**
   * `lagInFrame`/`leadInFrame`, which read a row offset away from the current one, clamped to the window frame. Note
   * that the default frame runs to the current row, so a lead needs an explicit frame to see anything.
   *
   * `default` is the value for rows whose offset falls outside the frame; without it the column's own default is used.
   * It is a [[com.crobox.clickhouse.dsl.Column]] rather than a `V` so an expression can be passed -- use `const(...)`
   * for a plain value. The server requires the supertype of the column's type and the default's to *be* the column's
   * type, which is stricter than it sounds: against a `UInt32` column `const(0)` is fine, but `const(-1)` widens to
   * `Int64` and `const(0d)` to `Float64`, and both are rejected with "is not the same as the argument type". Cast the
   * default when the value has to be out of the column's range. Not checked here, because `TableColumn` is covariant --
   * a mismatched pair would infer a common supertype for `V` rather than fail.
   */
  case class LagInFrame[V](
      column: TableColumn[V],
      offset: Option[Column] = None,
      default: Option[Column] = None
  ) extends WindowOnlyFunctionCol[V](column) {
    require(offset.isDefined || default.isEmpty, "lagInFrame needs an offset before a default")
  }

  case class LeadInFrame[V](
      column: TableColumn[V],
      offset: Option[Column] = None,
      default: Option[Column] = None
  ) extends WindowOnlyFunctionCol[V](column) {
    require(offset.isDefined || default.isEmpty, "leadInFrame needs an offset before a default")
  }

  /** `nth_value`, the value at a one-based position within the frame. */
  case class NthValue[V](column: TableColumn[V], offset: Long) extends WindowOnlyFunctionCol[V](column) {

    // The server rejects 0 with "The offset for function nth_value must be in (1, ...]".
    require(offset >= 1, s"nth_value offsets are one-based, got $offset")
  }

  /** Position within the partition, counting from 1, with no regard for ties. */
  def rowNumber(): WindowOnlyFunction[Long] = new WindowOnlyFunction(RowNumber())

  /** Position within the partition, where ties share a position and the next position skips the gap. */
  def rank(): WindowOnlyFunction[Long] = new WindowOnlyFunction(Rank())

  /** Position within the partition, where ties share a position and the next position does not skip. */
  def denseRank(): WindowOnlyFunction[Long] = new WindowOnlyFunction(DenseRank())

  /** [[rank]] scaled into `[0, 1]`. */
  def percentRank(): WindowOnlyFunction[Double] = new WindowOnlyFunction(PercentRank())

  /** Splits the partition into `buckets` groups of as near equal size as the row count allows. */
  def ntile(buckets: Long): WindowOnlyFunction[Long] = new WindowOnlyFunction(Ntile(buckets))

  def lagInFrame[V](
      column: TableColumn[V],
      offset: Option[Column] = None,
      default: Option[Column] = None
  ): WindowOnlyFunction[V] = new WindowOnlyFunction(LagInFrame(column, offset, default))

  def leadInFrame[V](
      column: TableColumn[V],
      offset: Option[Column] = None,
      default: Option[Column] = None
  ): WindowOnlyFunction[V] = new WindowOnlyFunction(LeadInFrame(column, offset, default))

  def nthValue[V](column: TableColumn[V], offset: Long): WindowOnlyFunction[V] =
    new WindowOnlyFunction(NthValue(column, offset))
}
