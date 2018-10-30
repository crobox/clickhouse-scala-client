package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Const, EmptyColumn, ExpressionColumn, TableColumn}

trait LogicalFunctions { this: Magnets =>

  abstract class LogicalFunction(val left: LogicalOpsMagnet, val right: LogicalOpsMagnet)
      extends ExpressionColumn[Boolean](EmptyColumn())

  trait LogicalOps { this: LogicalOpsMagnet =>
    def and(other: LogicalOpsMagnet) = _and(this, other)
    def or(other: LogicalOpsMagnet)  = _or(this, other)
    def xor(other: LogicalOpsMagnet) = _xor(this, other)
  }

  //Reference with another name to allow to use it in the trait
  private def _and = and _
  private def _or = or _
  private def _xor = xor _

  case class And(_left: LogicalOpsMagnet, _right: LogicalOpsMagnet) extends LogicalFunction(_left, _right)
  case class Or(_left: LogicalOpsMagnet, _right: LogicalOpsMagnet)  extends LogicalFunction(_left, _right)
  case class Xor(_left: LogicalOpsMagnet, _right: LogicalOpsMagnet) extends LogicalFunction(_left, _right)
  case class Not(col: NumericCol[_]) extends ExpressionColumn[Boolean](col.column)

  def and(_left: LogicalOpsMagnet, _right: LogicalOpsMagnet): TableColumn[Boolean] = {
    if (_left.asOption.isEmpty && _right.asOption.isEmpty )
      Const(true)
    else if (_left.isConstFalse || _right.isConstFalse)
      Const(false)
    else if ((_left.asOption.isEmpty || _left.isConstTrue) && _right.asOption.isDefined)
      _right.asOption.get
    else if ((_right.asOption.isEmpty || _right.isConstTrue) && _left.asOption.isDefined)
      _left.asOption.get
    else
      And(_left, _right)
  }
  def or(_left: LogicalOpsMagnet, _right: LogicalOpsMagnet)  = {
    if (_left.asOption.isEmpty && _right.asOption.isEmpty )
      Const(true)
    else if (_left.isConstTrue || _right.isConstTrue)
      Const(true)
    else if (_left.isConstFalse && _right.isConstFalse)
      Const(false)
    else if ((_left.asOption.isEmpty || _left.asOption.isConstFalse) && _right.asOption.isDefined)
      _right.asOption.get
    else if ((_right.asOption.isEmpty || _right.asOption.isConstFalse) && _left.asOption.isDefined)
      _left.asOption.get
    else
      Or(_left, _right)
  }
  def xor(_left: LogicalOpsMagnet, _right: LogicalOpsMagnet) = {
    if (_left.asOption.isEmpty && _right.asOption.isEmpty )
      Const(true)
    else if (_left.isConstTrue && _right.isConstTrue)
      Const(false)
    else if ((_left.isConstTrue && _right.isConstFalse) || (_right.isConstTrue && _left.isConstFalse))
      Const(true)
    else if (_left.asOption.isEmpty && _right.asOption.isDefined)
      _right.asOption.get
    else if (_right.asOption.isEmpty && _left.asOption.isDefined)
      _left.asOption.get
    else
      Xor(_left, _right)
  }
  def not(col: NumericCol[_]) = col.column match {
    case Const(true) => Const(false)
    case Const(false) => Const(true)
    case _ => Not(col)
  }
}
