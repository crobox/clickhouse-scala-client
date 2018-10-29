package com.crobox.clickhouse.dsl.column
import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, OperationalQuery}

trait InFunctions { self: Magnets =>

  sealed trait InFunction
  abstract class InFunctionCol[O](val l: ConstOrColMagnet[_], val r: InFuncRHMagnet) extends ExpressionColumn[Boolean](EmptyColumn()) with InFunction

  case class In(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet) extends InFunctionCol(_l, _r)
  case class NotIn(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet) extends InFunctionCol(_l, _r)
  case class GlobalIn(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet) extends InFunctionCol(_l, _r)
  case class GlobalNotIn(_l: ConstOrColMagnet[_], _r: InFuncRHMagnet) extends InFunctionCol(_l, _r)

  //FIXME: we lose types here,
  // is there anything that could properly represent the inner types of these column functions?
  //This is especially problematic when using TupleElement

  case class Tuple(coln: Seq[ConstOrColMagnet[_]]) extends ExpressionColumn[Nothing](EmptyColumn()) with InFunction
  case class TupleElement[T](tuple: Tuple, index: NumericCol[_]) extends ExpressionColumn[T](EmptyColumn()) with InFunction

  trait InOps { self: ConstOrColMagnet[_] =>
    def in(other: InFuncRHMagnet) = In(self,other)
    def notIn(other: InFuncRHMagnet) = NotIn(self,other)
    def globalIn(other: InFuncRHMagnet) = GlobalIn(self,other)
    def globalNotIn(other: InFuncRHMagnet) = GlobalNotIn(self,other)
  }

  def in(l: ConstOrColMagnet[_],r: InFuncRHMagnet) = In(l,r)
  def notIn(l: ConstOrColMagnet[_],r: InFuncRHMagnet) = NotIn(l,r)
  def globalIn(l: ConstOrColMagnet[_],r: InFuncRHMagnet) = GlobalIn(l,r)
  def globalNotIn(l: ConstOrColMagnet[_],r: InFuncRHMagnet) = GlobalNotIn(l,r)

  def tuple(coln: ConstOrColMagnet[_]*) : Tuple = Tuple(coln)
  def tupleElement[T](tuple: Tuple, index: NumericCol[_]): TupleElement[T] = TupleElement[T](tuple,index)
}
