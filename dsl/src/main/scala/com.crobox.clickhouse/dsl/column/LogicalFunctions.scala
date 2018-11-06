package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Const, EmptyColumn, ExpressionColumn}

trait LogicalFunctions { this: Magnets =>

  sealed trait LogicalOperator
  case object And extends LogicalOperator
  case object Or extends LogicalOperator
  case object Xor extends LogicalOperator
  case object Not extends LogicalOperator

  case class LogicalFunction(left: LogicalOpsMagnet, operator: LogicalOperator, right: LogicalOpsMagnet)
      extends ExpressionColumn[Boolean](EmptyColumn())

  trait LogicalOps { this: LogicalOpsMagnet =>
    def and(other: LogicalOpsMagnet): ExpressionColumn[Boolean] = _and(this, other)
    def or(other: LogicalOpsMagnet): ExpressionColumn[Boolean] = _or(this, other)
    def xor(other: LogicalOpsMagnet): ExpressionColumn[Boolean] = _xor(this, other)
    def not(other: LogicalOpsMagnet): ExpressionColumn[Boolean] = _not(other)
  }

  //Reference with another name to allow to use it in the trait
  private def _and = and _
  private def _or = or _
  private def _xor = xor _
  private def _not = not _

  def and(left: LogicalOpsMagnet, right: LogicalOpsMagnet): ExpressionColumn[Boolean] = {
    (left.asOption, right.asOption) match {
      case (None, None) => Const(true)
      case (Some(Const(false)), _) => Const(false)
      case (_, Some(Const(false))) => Const(false)
      case (Some(Const(true)), Some(Const(true))) => Const(true)
      case (_, _) => LogicalFunction(left, And, right)
    }
  }
  def or(left: LogicalOpsMagnet, right: LogicalOpsMagnet): ExpressionColumn[Boolean] = {
    (left.asOption, right.asOption) match {
      case (None, None) => Const(true)
      case (Some(Const(true)), _) => Const(true)
      case (_, Some(Const(true))) => Const(true)
      case (Some(Const(false)), Some(Const(false))) => Const(false)
      case (_, _) => LogicalFunction(left, Or, right)
    }
  }
  def xor(left: LogicalOpsMagnet, right: LogicalOpsMagnet): ExpressionColumn[Boolean] = {
    (left.asOption, right.asOption) match {
      case (None, None) => Const(true)
      case (Some(Const(false)), Some(Const(true))) => Const(true)
      case (Some(Const(true)), Some(Const(false))) => Const(true)
      case (Some(Const(true)), Some(Const(true))) => Const(false)
      case (_, _) => LogicalFunction(left, Xor, right)
    }
  }
  def not(col: LogicalOpsMagnet): ExpressionColumn[Boolean] = {
    col.asOption match {
      case Some(Const(true)) => Const(false)
      case Some(Const(false)) => Const(true)
      case _ => LogicalFunction(col, Not, col) //Needs both right and left for the tokenizer
    }
  }
}
