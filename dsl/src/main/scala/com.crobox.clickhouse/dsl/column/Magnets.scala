package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.marshalling.QueryValue
import com.crobox.clickhouse.dsl.{Const, TableColumn}
import org.joda.time.{DateTime, LocalDate}


trait Magnets { self: ArithmeticFunctions with ComparisonFunctions =>

  /**
    * Magnet pattern
    *
    * The pattern provides implicit conversion to wrapper classes,
    * this allows the DSL to accept multiple compatible column types in a single function.
    */
  trait Magnet {
    type ColumnType
    val column: TableColumn[_]
  }

  sealed trait ArrayColMagnet extends Magnet

  implicit def arrayColMagnetFromIterable[T <: Iterable[_] : QueryValue](s: T) =
    new ArrayColMagnet {
      override type ColumnType = T
      override val column = Const(s)
    }

  implicit def arrayColMagnetFromIterableCol[S <: Iterable[_], T <: TableColumn[S]](s: T) =
    new ArrayColMagnet {
      override type ColumnType = S
      override val column = s
    }

  sealed trait StringColMagnet extends Magnet with HexCompatible with ComparableWith[StringColMagnet]

  implicit def stringColMagnetFromString[T <: String : QueryValue](s: T) =
    new StringColMagnet {
      override type ColumnType = String
      override val column: TableColumn[String] = Const(s)
    }

  implicit def stringColMagnetFromStringCol[T <: TableColumn[String]](s: T) =
    new StringColMagnet {
      override type ColumnType = String
      override val column: TableColumn[String] = s
    }

  trait AddSubtractAble extends Magnet with HexCompatible {
    def +(other: AddSubtractAble) = Plus(this, other)
    def -(other: AddSubtractAble) = Minus(this, other)
  }

  sealed trait HexCompatible extends Magnet

  sealed trait DateOrDateTime extends Magnet with AddSubtractAble with ComparableWith[DateOrDateTime]

  implicit def ddtFromDateCol[T <: TableColumn[LocalDate]](s: T) =
    new DateOrDateTime {
      override type ColumnType = LocalDate
      override val column = s
    }

  implicit def ddtFromDateTimeCol[T <: TableColumn[DateTime]](s: T) =
    new DateOrDateTime {
      override type ColumnType = DateTime
      override val column = s
    }

  implicit def ddtFromDate[T <: LocalDate : QueryValue](s: T) =
    new DateOrDateTime {
      override type ColumnType = LocalDate
      override val column = Const(s)
    }

  implicit def ddtFromDateTime[T <: DateTime : QueryValue](s: T) =
    new DateOrDateTime {
      override type ColumnType = DateTime
      override val column = Const(s)
    }

  sealed trait NumericCol extends Magnet with AddSubtractAble with ComparableWith[NumericCol] {
    def *(other: NumericCol) = Multiply(this, other)
    def /(other: NumericCol) = Divide(this, other)
    def %(other: NumericCol) = Modulo(this, other)
  }

  implicit def numericFromLong[T <: Long : QueryValue](s: T) =
    new NumericCol {
      override type ColumnType = Long
      override val column = Const(s)
    }

  implicit def numericFromInt[T <: Int : QueryValue](s: T) =
    new NumericCol {
      override type ColumnType = Int
      override val column = Const(s)
    }

  implicit def numericFromDouble[T <: Double : QueryValue](s: T) =
    new NumericCol {
      override type ColumnType = Double
      override val column = Const(s)
    }

  implicit def numericFromFloat[T <: Float : QueryValue](s: T) =
    new NumericCol {
      override type ColumnType = Float
      override val column = Const(s)
    }

  implicit def numericFromBigInt[T <: BigInt : QueryValue](s: T) =
    new NumericCol {
      override type ColumnType = BigInt
      override val column = Const(s)
    }

  implicit def numericFromBigDecimal[T <: BigDecimal : QueryValue](s: T) =
    new NumericCol {
      override type ColumnType = BigDecimal
      override val column = Const(s)
    }

  implicit def numericFromLongCol[T <: TableColumn[Long]](s: T) =
    new NumericCol {
      override type ColumnType = Long
      override val column = s
    }

  implicit def numericFromIntCol[T <: TableColumn[Int]](s: T) =
    new NumericCol {
      override type ColumnType = Int
      override val column = s
    }

  implicit def numericFromDoubleCol[T <: TableColumn[Double]](s: T) =
    new NumericCol {
      override type ColumnType = Double
      override val column = s
    }

  implicit def numericFromFloatCol[T <: TableColumn[Float]](s: T) =
    new NumericCol {
      override type ColumnType = Float
      override val column = s
    }

  implicit def numericFromBigIntCol[T <: TableColumn[BigInt]](s: T) =
    new NumericCol {
      override type ColumnType = BigInt
      override val column = s
    }

  implicit def numericFromBigDecimalCol[T <: TableColumn[BigDecimal]](s: T) =
    new NumericCol {
      override type ColumnType = BigDecimal
      override val column = s
    }


  sealed trait EmptyNonEmptyCol extends Magnet

  implicit def emptyNonEmptyFromStringCol[T <: TableColumn[String]](s: T) =
    new EmptyNonEmptyCol {
      override type ColumnType = String
      override val column: TableColumn[String] = s
    }

  implicit def emptyNonEmptyFromIterableCol[C <: Iterable[_], T <: TableColumn[C]](s: T) =
    new EmptyNonEmptyCol {
      override type ColumnType = C
      override val column: TableColumn[C] = s
    }

  implicit def emptyNonEmptyFromString[T <: String : QueryValue](s: T) =
    new EmptyNonEmptyCol {
      override type ColumnType = String
      override val column = Const(s)
    }

  implicit def emptyNonEmptyFromIterable[T <: Iterable[_] : QueryValue](s: T) =
    new EmptyNonEmptyCol {
      override type ColumnType = Iterable[_]
      override val column = Const(s)
    }

}
