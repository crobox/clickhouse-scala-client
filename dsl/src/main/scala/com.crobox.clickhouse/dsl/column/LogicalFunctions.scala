package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait LogicalFunctions { this: Magnets =>
  //TODO remove the original logic

  abstract class LogicalFunction(val left: NumericCol, val right: NumericCol)
      extends ExpressionColumn[Boolean](EmptyColumn())

  trait LogicalOps { this: NumericCol =>
    def AND(other: NumericCol) = And(this, other)
    def OR(other: NumericCol)  = Or(this, other)
    def XOR(other: NumericCol) = Xor(this, other)
  }

  case class And(_left: NumericCol, _right: NumericCol) extends LogicalFunction(_left, _right)
  case class Or(_left: NumericCol, _right: NumericCol)  extends LogicalFunction(_left, _right)
  case class Xor(_left: NumericCol, _right: NumericCol) extends LogicalFunction(_left, _right)

  def and(_left: NumericCol, _right: NumericCol) = And(_left, _right)
  def or(_left: NumericCol, _right: NumericCol)  = Or(_left, _right)
  def xor(_left: NumericCol, _right: NumericCol) = Xor(_left, _right)

  /*
  and()
  or()
  xor()

  not()
   */ //TODO rethink the not operator?

}
