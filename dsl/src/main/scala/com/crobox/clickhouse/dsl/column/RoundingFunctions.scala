package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait RoundingFunctions { self: Magnets =>
  abstract class RoundingFunction(col: NumericCol[_]) extends ExpressionColumn[Long](col.column)

  case class Floor(col: NumericCol[_], n: NumericCol[_]) extends RoundingFunction(col)
  case class Ceil(col: NumericCol[_], n: NumericCol[_])  extends RoundingFunction(col)
  case class Round(col: NumericCol[_], n: NumericCol[_]) extends RoundingFunction(col)
  case class RoundToExp2(col: NumericCol[_])             extends RoundingFunction(col)
  case class RoundDuration(col: NumericCol[_])           extends RoundingFunction(col)
  case class RoundAge(col: NumericCol[_])                extends RoundingFunction(col)

  def floor(col: NumericCol[_], n: NumericCol[_]) = Floor(col, n)
  def ceil(col: NumericCol[_], n: NumericCol[_])  = Ceil(col, n)
  def round(col: NumericCol[_], n: NumericCol[_]) = Round(col, n)
  def roundToExp2(col: NumericCol[_])             = RoundToExp2(col)
  def roundDuration(col: NumericCol[_])           = RoundDuration(col)
  def roundAge(col: NumericCol[_])                = RoundAge(col)

  /*
floor(x[, N])
ceil(x[, N])
round(x[, N])
roundToExp2(num)
roundDuration(num)
roundAge(num)
   */
}
