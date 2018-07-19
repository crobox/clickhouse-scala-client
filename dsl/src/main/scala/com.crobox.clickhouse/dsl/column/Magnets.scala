package com.crobox.clickhouse.dsl.column

import java.util.UUID

import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}
import com.crobox.clickhouse.dsl.{Const, TableColumn}
import org.joda.time.{DateTime, LocalDate}

trait Magnets { self: ArithmeticFunctions with ComparisonFunctions with LogicalFunctions =>

  /**
    * Magnet pattern
    *
    * The pattern provides implicit conversion to wrapper classes,
    * this allows the DSL to accept multiple compatible column types in a single function.
    */
  trait Magnet[C] {
    val column: TableColumn[C]
  }

  sealed trait ConstOrColMagnet[C] extends Magnet[C]

  implicit def constOrColMagnetFromCol[C](s: TableColumn[C]) =
    new ConstOrColMagnet[C] {
      override val column = s
    }

  implicit def constOrColMagnetFromConst[T : QueryValue](s: T) =
    new ConstOrColMagnet[T] {
      override val column = Const(s)
    }

  sealed trait ArrayColMagnet[C] extends Magnet[C]

  implicit def arrayColMagnetFromIterable[T : QueryValue](s: Iterable[T]) =
    new ArrayColMagnet[Iterable[T]] {

      val qvForIterable = QueryValueFormats.queryValueToSeq(implicitly[QueryValue[T]])

      override val column = Const(s)(qvForIterable)
    }

  implicit def arrayColMagnetFromIterableCol[S <: Iterable[_], T <: TableColumn[S]](s: T) =
    new ArrayColMagnet[S] {
      override val column = s
    }

  sealed trait StringColMagnet[C] extends Magnet[C] with HexCompatible[C] with ComparableWith[StringColMagnet]

  implicit def stringColMagnetFromString[T <: String : QueryValue](s: T) =
    new StringColMagnet[String] {
      override val column: TableColumn[String] = Const(s)
    }

  implicit def stringColMagnetFromStringCol[T <: TableColumn[String]](s: T) =
    new StringColMagnet[String] {
      override val column: TableColumn[String] = s
    }

  implicit def stringColMagnetFromUUID[T <: UUID : QueryValue](s: T) =
    new StringColMagnet[UUID] {
      override val column: TableColumn[UUID] = Const(s)
    }

  implicit def stringColMagnetFromUUIDCol[T <: TableColumn[UUID]](s: T) =
    new StringColMagnet[UUID] {
      override val column: TableColumn[UUID] = s
    }

  sealed trait HexCompatible[C] extends Magnet[C]

  sealed trait DateOrDateTime[C] extends Magnet[C] with AddSubtractable[C] with ComparableWith[DateOrDateTime[_]]

  implicit def ddtFromDateCol[T <: TableColumn[LocalDate]](s: T) =
    new DateOrDateTime[LocalDate] {
      override val column = s
    }

  implicit def ddtFromDateTimeCol[T <: TableColumn[DateTime]](s: T) =
    new DateOrDateTime[DateTime] {
      override val column = s
    }

  implicit def ddtFromDate[T <: LocalDate : QueryValue](s: T) =
    new DateOrDateTime[LocalDate] {
      override val column = Const(s)
    }

  implicit def ddtFromDateTime[T <: DateTime : QueryValue](s: T) =
    new DateOrDateTime[DateTime] {
      override val column = Const(s)
    }

  sealed trait AddSubtractable[C] extends Magnet[C] with AddSubtractOps[C]

  sealed trait NumericCol[C] extends Magnet[C] with AddSubtractable[C] with ComparableWith[NumericCol[_]] with LogicalOps with ArithmeticOps[C]
//  sealed trait NumericColTC[N] extends ComparableWith[NumericColTC[_]] with LogicalOpsTC
//
//  trait LogicalOpsTC { this: NumericColTC[_] =>
//    def AND[T : NumericColTC](other: T) = const(1)
//  }
//
//  implicit class IntNumericCol extends NumericColTC[Int] {
//     def column(in: Int): TableColumn[Int] = const(in)
//  }

  implicit def numericFromLong[T <: Long : QueryValue](s: T) =
    new NumericCol[T] {
      override val column = Const(s)
    }

  implicit def numericFromInt[T <: Int : QueryValue](s: T) =
    new NumericCol[T] {
      override val column = Const(s)
    }

  implicit def numericFromDouble[T <: Double : QueryValue](s: T) =
    new NumericCol[T] {
      override val column = Const(s)
    }

  implicit def numericFromFloat[T <: Float : QueryValue](s: T) =
    new NumericCol[T] {
      override val column = Const(s)
    }

  implicit def numericFromBigInt[T <: BigInt : QueryValue](s: T) =
    new NumericCol[T] {
      override val column = Const(s)
    }

  implicit def numericFromBigDecimal[T <: BigDecimal : QueryValue](s: T) =
    new NumericCol[T] {
      override val column = Const(s)
    }

  implicit def numericFromBoolean[T <: Boolean : QueryValue](s: T) =
    new NumericCol[T] {
      override val column = Const(s)
    }

  implicit def numericFromLongCol[T <: TableColumn[Long]](s: T) =
    new NumericCol[Long] {
      override val column = s
    }

  implicit def numericFromIntCol[T <: TableColumn[Int]](s: T) =
    new NumericCol[Int] {
      override val column = s
    }

  implicit def numericFromDoubleCol[T <: TableColumn[Double]](s: T) =
    new NumericCol[Double] {
      override val column = s
    }

  implicit def numericFromFloatCol[T <: TableColumn[Float]](s: T) =
    new NumericCol[Float] {
      override val column = s
    }

  implicit def numericFromBigIntCol[T <: TableColumn[BigInt]](s: T) =
    new NumericCol[BigInt] {
      override val column = s
    }

  implicit def numericFromBigDecimalCol[T <: TableColumn[BigDecimal]](s: T) =
    new NumericCol[BigDecimal] {
      override val column = s
    }

  implicit def numericFromBooleanCol[T <: TableColumn[Boolean]](s: T) =
    new NumericCol[Boolean] {
      override val column = s
    }

  //Marks types that can be checked on empty/nonempty and length (atleast collections and strings)
  sealed trait EmptyNonEmptyCol[C] extends Magnet[C]

  implicit def emptyNonEmptyFromStringCol[T <: TableColumn[String]](s: T) =
    new EmptyNonEmptyCol[String] {
      override val column: TableColumn[String] = s
    }

  implicit def emptyNonEmptyFromIterableCol[C <: Iterable[_], T <: TableColumn[C]](s: T) =
    new EmptyNonEmptyCol[C] {
      override val column: TableColumn[C] = s
    }

  implicit def emptyNonEmptyFromString[T <: String : QueryValue](s: T) =
    new EmptyNonEmptyCol[String] {
      override val column = Const(s)
    }

  implicit def emptyNonEmptyFromIterable[T <: Iterable[_] : QueryValue](s: T) =
    new EmptyNonEmptyCol[T] {
      override val column = Const(s)
    }

}
