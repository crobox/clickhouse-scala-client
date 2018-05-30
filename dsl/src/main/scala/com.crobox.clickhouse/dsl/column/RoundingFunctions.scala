package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait RoundingFunctions { self: Magnets =>
  abstract class RoundingFunction(col: NumericCol) extends ExpressionColumn[Long](col.column)

  case class Floor(col: NumericCol, n: NumericCol) extends RoundingFunction(col)
  case class Ceil(col: NumericCol, n: NumericCol) extends RoundingFunction(col)
  case class Round(col: NumericCol, n: NumericCol) extends RoundingFunction(col)
  case class RoundToExp2(col: NumericCol) extends RoundingFunction(col)
  case class RoundDuration(col: NumericCol) extends RoundingFunction(col)
  case class RoundAge(col: NumericCol) extends RoundingFunction(col)

  def floor(col: NumericCol, n: NumericCol) = Floor(col, n)
  def ceil(col: NumericCol, n: NumericCol) = Ceil(col, n)
  def round(col: NumericCol, n: NumericCol) = Round(col, n)
  def roundToExp2(col: NumericCol) = RoundToExp2(col)
  def roundDuration(col: NumericCol) = RoundDuration(col)
  def roundAge(col: NumericCol) = RoundAge(col)

/*
floor(x[, N])
ceil(x[, N])
round(x[, N])
roundToExp2(num)
roundDuration(num)
roundAge(num)
 */
}
