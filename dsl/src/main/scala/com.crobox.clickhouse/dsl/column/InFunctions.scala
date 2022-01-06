package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Const, EmptyColumn, ExpressionColumn}

trait InFunctions { self: Magnets =>

  sealed trait InFunction
  abstract class InFunctionCol[O](val l: ConstOrColMagnet[_], val r: InFuncRHMagnet)
      extends ExpressionColumn[Boolean](EmptyColumn)
      with InFunction

  case class In(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet)          extends InFunctionCol(_l, _r)
  case class NotIn(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet)       extends InFunctionCol(_l, _r)
  case class GlobalIn(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet)    extends InFunctionCol(_l, _r)
  case class GlobalNotIn(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet) extends InFunctionCol(_l, _r)

  //FIXME: we lose types here,
  // is there anything that could properly represent the inner types of these column functions?
  //This is especially problematic when using TupleElement

  case class Tuple(coln: Seq[ConstOrColMagnet[_]]) extends ExpressionColumn[Nothing](EmptyColumn) with InFunction
  case class TupleElement[T](tuple: Tuple, index: NumericCol[_])
      extends ExpressionColumn[T](EmptyColumn)
      with InFunction

  trait InOps { self: ConstOrColMagnet[_] =>
    def in(other: InFuncRHMagnet): In                   = In(self, other)
    def notIn(other: InFuncRHMagnet): NotIn             = NotIn(self, other)
    def globalIn(other: InFuncRHMagnet): GlobalIn       = GlobalIn(self, other)
    def globalNotIn(other: InFuncRHMagnet): GlobalNotIn = GlobalNotIn(self, other)

    def in(other: InFuncRHMagnet, global: Boolean): InFunctionCol[_] = if (global) globalIn(other) else in(other)

    def notIn(other: InFuncRHMagnet, global: Boolean): InFunctionCol[_] =
      if (global) globalNotIn(other) else notIn(other)
  }

  def in(l: ConstOrColMagnet[_], r: InFuncRHMagnet): ExpressionColumn[Boolean] =
    if (r.isEmptyCollection) Const(false) else In(l, r)

  def notIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet): ExpressionColumn[Boolean] =
    if (r.isEmptyCollection) Const(true) else NotIn(l, r)

  def globalIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet): ExpressionColumn[Boolean] =
    if (r.isEmptyCollection) Const(false) else GlobalIn(l, r)

  def globalNotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet): ExpressionColumn[Boolean] =
    if (r.isEmptyCollection) Const(true) else GlobalNotIn(l, r)

  def in(l: ConstOrColMagnet[_], r: InFuncRHMagnet, global: Boolean): ExpressionColumn[Boolean] =
    if (global) globalIn(l, r) else in(l, r)

  def notIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet, global: Boolean): ExpressionColumn[Boolean] =
    if (global) globalNotIn(l, r) else notIn(l, r)

  def tuple(coln: ConstOrColMagnet[_]*): Tuple                             = Tuple(coln)
  def tupleElement[T](tuple: Tuple, index: NumericCol[_]): TupleElement[T] = TupleElement[T](tuple, index)
}
