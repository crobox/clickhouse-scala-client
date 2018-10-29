package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait LogicalFunctions { this: Magnets =>
  //TODO remove the original logic

  abstract class LogicalFunction(val left: NumericCol[_], val right: NumericCol[_])
      extends ExpressionColumn[Boolean](EmptyColumn())

  trait LogicalOps { this: NumericCol[_] =>
    def and(other: NumericCol[_]) = And(this, other)
    def or(other: NumericCol[_])  = Or(this, other)
    def xor(other: NumericCol[_]) = Xor(this, other)
  }

  case class And(_left: NumericCol[_], _right: NumericCol[_]) extends LogicalFunction(_left, _right)
  case class Or(_left: NumericCol[_], _right: NumericCol[_])  extends LogicalFunction(_left, _right)
  case class Xor(_left: NumericCol[_], _right: NumericCol[_]) extends LogicalFunction(_left, _right)
  case class Not(col: NumericCol[_]) extends ExpressionColumn[Boolean](col.column)

  def and(_left: NumericCol[_], _right: NumericCol[_]) = And(_left, _right)
  def or(_left: NumericCol[_], _right: NumericCol[_])  = Or(_left, _right)
  def xor(_left: NumericCol[_], _right: NumericCol[_]) = Xor(_left, _right)
  def not(col: NumericCol[_]) = Not(col)

  /*
  and()
  or()
  xor()

  not()
   */ //TODO rethink the not operator?

}
