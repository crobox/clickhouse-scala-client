package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn, TableColumn}
import org.joda.time.{DateTime, LocalDate}
import scala.language.implicitConversions

trait ArithmeticFunctions { self: Magnets =>

  sealed trait ArithmeticFunction

  abstract class ArithmeticFunctionCol[V](val numericCol: NumericCol[_])
      extends ExpressionColumn[V](numericCol.column)
      with ArithmeticFunction
      with NumericCol[V] {
    override val column: TableColumn[V] = this
  }

  abstract class ArithmeticFunctionOp[V](val left: AddSubtractable[_], val right: AddSubtractable[_])
      extends ExpressionColumn[V](EmptyColumn)
      with ArithmeticFunction
      with NumericCol[V] {
    override val column: TableColumn[V] = this
  }

  trait AddSubtractOps[L] { self: AddSubtractable[_] =>
    def +[R, O](other: AddSubtractable[R])(implicit ev: AritRetType[L, R, O]): Plus[O]  = Plus[O](this, other)
    def -[R, O](other: AddSubtractable[R])(implicit ev: AritRetType[L, R, O]): Minus[O] = Minus[O](this, other)
  }

  trait ArithmeticOps[L] { self: NumericCol[_] =>
    def *[R, O](other: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Multiply[O] = Multiply[O](this, other)
    def /[R, O](other: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Divide[O]   = Divide[O](this, other)
    def %[R, O](other: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Modulo[O]   = Modulo[O](this, other)
    def ^[R, O](other: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Power[O]    = Power[O](this, other)
  }

  case class Plus[T](l: AddSubtractable[_], r: AddSubtractable[_])  extends ArithmeticFunctionOp[T](l, r)
  case class Minus[T](l: AddSubtractable[_], r: AddSubtractable[_]) extends ArithmeticFunctionOp[T](l, r)
  case class Multiply[T](l: NumericCol[_], r: NumericCol[_])        extends ArithmeticFunctionOp[T](l, r)
  case class Divide[T](l: NumericCol[_], r: NumericCol[_])          extends ArithmeticFunctionOp[T](l, r)
  case class IntDiv[T](l: NumericCol[_], r: NumericCol[_])          extends ArithmeticFunctionOp[T](l, r)
  case class IntDivOrZero[T](l: NumericCol[_], r: NumericCol[_])    extends ArithmeticFunctionOp[T](l, r)
  case class Modulo[T](l: NumericCol[_], r: NumericCol[_])          extends ArithmeticFunctionOp[T](l, r)
  case class Power[T](l: NumericCol[_], r: NumericCol[_])           extends ArithmeticFunctionOp[T](l, r)
  case class Gcd[T](l: NumericCol[_], r: NumericCol[_])             extends ArithmeticFunctionOp[T](l, r)
  case class Lcm[T](l: NumericCol[_], r: NumericCol[_])             extends ArithmeticFunctionOp[T](l, r)

  case class Negate[T](t: NumericCol[T]) extends ArithmeticFunctionCol[T](t)
  case class Abs[T](t: NumericCol[T])    extends ArithmeticFunctionCol[T](t)

  //trait ArithmeticFunctionsDsl {

  sealed abstract class AritRetType[L, R, O]
  implicit object IntIntBinding        extends AritRetType[Int, Int, Int]
  implicit object IntLongBinding       extends AritRetType[Int, Long, Long]
  implicit object IntDoubleBinding     extends AritRetType[Int, Double, Double]
  implicit object IntFloatBinding      extends AritRetType[Int, Float, Float]
  implicit object IntBigDecimalBinding extends AritRetType[Int, BigDecimal, BigDecimal]
  implicit object IntBigIntBinding     extends AritRetType[Int, BigInt, BigInt]

  implicit object LongIntBinding        extends AritRetType[Long, Int, Int]
  implicit object LongLongBinding       extends AritRetType[Long, Long, Long]
  implicit object LongDoubleBinding     extends AritRetType[Long, Double, Double]
  implicit object LongFloatBinding      extends AritRetType[Long, Float, Float]
  implicit object LongBigDecimalBinding extends AritRetType[Long, BigDecimal, BigDecimal]
  implicit object LongBigIntBinding     extends AritRetType[Long, BigInt, BigInt]

  implicit object DoubleIntBinding        extends AritRetType[Double, Int, Double]
  implicit object DoubleLongBinding       extends AritRetType[Double, Long, Double]
  implicit object DoubleDoubleBinding     extends AritRetType[Double, Double, Double]
  implicit object DoubleFloatBinding      extends AritRetType[Double, Float, Float]
  implicit object DoubleBigDecimalBinding extends AritRetType[Double, BigDecimal, BigDecimal]
  implicit object DoubleBigIntBinding     extends AritRetType[Double, BigInt, BigDecimal]

  implicit object FloatIntBinding        extends AritRetType[Float, Int, Float]
  implicit object FloatLongBinding       extends AritRetType[Float, Long, Float]
  implicit object FloatDoubleBinding     extends AritRetType[Float, Double, Float]
  implicit object FloatFloatBinding      extends AritRetType[Float, Float, Float]
  implicit object FloatBigDecimalBinding extends AritRetType[Float, BigDecimal, BigDecimal]
  implicit object FloatBigIntBinding     extends AritRetType[Float, BigInt, BigDecimal]

  implicit object BigDecimalIntBinding        extends AritRetType[BigDecimal, Int, BigDecimal]
  implicit object BigDecimalLongBinding       extends AritRetType[BigDecimal, Long, BigDecimal]
  implicit object BigDecimalDoubleBinding     extends AritRetType[BigDecimal, Double, BigDecimal]
  implicit object BigDecimalFloatBinding      extends AritRetType[BigDecimal, Float, BigDecimal]
  implicit object BigDecimalBigDecimalBinding extends AritRetType[BigDecimal, BigDecimal, BigDecimal]
  implicit object BigDecimalBigIntBinding     extends AritRetType[BigDecimal, BigInt, BigDecimal]

  implicit object BigIntIntBinding        extends AritRetType[BigInt, Int, BigInt]
  implicit object BigIntLongBinding       extends AritRetType[BigInt, Long, BigInt]
  implicit object BigIntDoubleBinding     extends AritRetType[BigInt, Double, BigDecimal]
  implicit object BigIntFloatBinding      extends AritRetType[BigInt, Float, BigDecimal]
  implicit object BigIntBigDecimalBinding extends AritRetType[BigInt, BigDecimal, BigDecimal]
  implicit object BigIntBigIntBinding     extends AritRetType[BigInt, BigInt, BigInt]

  implicit object LocalDateIntBinding        extends AritRetType[LocalDate, Int, LocalDate]
  implicit object LocalDateLongBinding       extends AritRetType[LocalDate, Long, LocalDate]
  implicit object LocalDateDoubleBinding     extends AritRetType[LocalDate, Double, LocalDate]
  implicit object LocalDateFloatBinding      extends AritRetType[LocalDate, Float, LocalDate]
  implicit object LocalDateBigDecimalBinding extends AritRetType[LocalDate, BigDecimal, LocalDate]
  implicit object LocalDateBigIntBinding     extends AritRetType[LocalDate, BigInt, LocalDate]

  implicit object DateTimeIntBinding        extends AritRetType[DateTime, Int, DateTime]
  implicit object DateTimeLongBinding       extends AritRetType[DateTime, Long, DateTime]
  implicit object DateTimeDoubleBinding     extends AritRetType[DateTime, Double, DateTime]
  implicit object DateTimeFloatBinding      extends AritRetType[DateTime, Float, DateTime]
  implicit object DateTimeBigDecimalBinding extends AritRetType[DateTime, BigDecimal, DateTime]
  implicit object DateTimeBigIntBinding     extends AritRetType[DateTime, BigInt, DateTime]

  def abs[T](targetColumn: NumericCol[T]): Abs[T] = Abs[T](targetColumn)

  def divide[L, R, O](left: NumericCol[L], right: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Divide[O] =
    Divide[O](left, right)

  def gcd[L, R, O](left: NumericCol[L], right: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Gcd[O] =
    Gcd[O](left, right)

  def intDiv[L, R, O](left: NumericCol[L], right: NumericCol[R])(implicit ev: AritRetType[L, R, O]): IntDiv[O] =
    IntDiv[O](left, right)

  def intDivOrZero[L, R, O](left: NumericCol[L], right: NumericCol[R])(
      implicit ev: AritRetType[L, R, O]
  ): IntDivOrZero[O] = IntDivOrZero[O](left, right)

  def lcm[L, R, O](left: NumericCol[L], right: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Lcm[O] =
    Lcm[O](left, right)

  def minus[L, R, O](left: AddSubtractable[L], right: AddSubtractable[R])(implicit ev: AritRetType[L, R, O]): Minus[O] =
    Minus[O](left, right)

  def modulo[L, R, O](left: NumericCol[L], right: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Modulo[O] =
    Modulo[O](left, right)

  def multiply[L, R, O](left: NumericCol[L], right: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Multiply[O] =
    Multiply[O](left, right)

  def negate[T](targetColumn: NumericCol[T]): Negate[T] = Negate[T](targetColumn)

  def plus[L, R, O](left: AddSubtractable[L], right: AddSubtractable[R])(implicit ev: AritRetType[L, R, O]): Plus[O] =
    Plus[O](left, right)

  def power[L, R, O](left: NumericCol[L], right: NumericCol[R])(implicit ev: AritRetType[L, R, O]): Power[O] =
    Power[O](left, right)
}
