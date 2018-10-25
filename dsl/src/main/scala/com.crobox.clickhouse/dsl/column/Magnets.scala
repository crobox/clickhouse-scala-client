package com.crobox.clickhouse.dsl.column

import java.util.UUID

import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}
import com.crobox.clickhouse.dsl.{Const, EmptyColumn, OperationalQuery, Table, TableColumn}
import org.joda.time.{DateTime, LocalDate}

trait Magnets { self:
  ArithmeticFunctions with
  ComparisonFunctions with
  LogicalFunctions with
  TypeCastFunctions with
  StringFunctions with
  StringSearchFunctions with
  ScalaBooleanFunctions with
  ScalaStringFunctions with
  InFunctions =>

  /**
    * Magnet pattern
    *
    * The pattern provides implicit conversion to wrapper classes,
    * this allows the DSL to accept multiple compatible column types in a single function.
    */
  trait Magnet[C] {
    val column: TableColumn[C]
  }

  /**
    * Any constant or column.
    * Sidenote: The current implementation doesn't represent collections.
    */
  trait ConstOrColMagnet[C] extends Magnet[C] with ScalaBooleanFunctionOps[C] with InOps

  implicit def constOrColMagnetFromCol[C](s: TableColumn[C]) =
    new ConstOrColMagnet[C] {
      override val column = s
    }

  implicit def constOrColMagnetFromConst[T : QueryValue](s: T) =
    new ConstOrColMagnet[T] {
      override val column = Const(s)
    }


  sealed trait TupleColMagnet extends Magnet[Nothing]

//  implicit def TupleColMagnetFromIterable[T : QueryValue](s: Iterable[T]) =
//    new InFuncRHMagnet {
//      val qvT = implicitly[QueryValue[T]]
//      val sConsts: Seq[ConstOrColMagnet[T]] = s.map(col => constOrColMagnetFromConst(col)(qvT)).toSeq
//
//      override val column = tuple(sConsts:_*)
//    }

  /**
    * Represents any accepted type for the right hand argument of the IN operators (tuple, table or Qry)
    */
  sealed trait InFuncRHMagnet extends Magnet[Nothing] {
    val query: Option[OperationalQuery] = None
    val tableRef: Option[Table] = None
  }

  implicit def InFuncRHMagnetFromIterable[T : QueryValue](s: Iterable[T]) =
    new InFuncRHMagnet {
      val qvT = implicitly[QueryValue[T]]
      val sConsts: Seq[ConstOrColMagnet[T]] = s.map(col => constOrColMagnetFromConst(col)(qvT)).toSeq

      override val column = tuple(sConsts:_*)
    }

  implicit def InFuncRHMagnetFromTuple(s: Tuple) =
    new InFuncRHMagnet {
      override val column = s
    }

  implicit def InFuncRHMagnetFromQuery(s: OperationalQuery) =
    new InFuncRHMagnet {
      override val column = EmptyColumn()
      override val tableRef: Option[Table] = None
      override val query: Option[OperationalQuery] = Some(s)
    }

  implicit def InFuncRHMagnetFromTable(s: Table) =
    new InFuncRHMagnet {
      override val column = EmptyColumn()
      override val tableRef: Option[Table] = Some(s)
    }
  

  /**
    * Represents any collection
    */
  sealed trait ArrayColMagnet[C] extends Magnet[C]

  implicit def arrayColMagnetFromIterable[T : QueryValue](s: Iterable[T]) =
    new ArrayColMagnet[Iterable[T]] {

      val qvForIterable = QueryValueFormats.queryValueToSeq(implicitly[QueryValue[T]])

      override val column = Const(s)(qvForIterable)
    }

  implicit def arrayColMagnetFromIterableCol[Elem, Collection[B] <: Iterable[B], ColType[A] <: TableColumn[A]](s: ColType[Collection[Elem]]) =
    new ArrayColMagnet[Collection[Elem]] {
      override val column = s
    }
  /**
    * String-like columns
    */
  trait StringColMagnet[C] extends Magnet[C] with HexCompatible[C] with ComparableWith[StringColMagnet[_]] with ScalaStringFunctionOps with StringOps with StringSearchOps with EmptyNonEmptyCol[C]

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

  /**
    * Types that are compatible with HEX accepting functions
    */
  sealed trait HexCompatible[C] extends Magnet[C]

  /**
    * Date or date time representations
    */
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
      override val column = toDate(s)
    }

  implicit def ddtFromDateTime[T <: DateTime : QueryValue](s: T) =
    new DateOrDateTime[DateTime] {
      override val column = toDateTime(s)
    }

  /**
    * Type that is expected by functions that shall then add or subtract from this value.
    *
    * These are not just numerics, but f.i. also dates / times.
    */
  sealed trait AddSubtractable[C] extends Magnet[C] with AddSubtractOps[C]

  trait NumericCol[C] extends Magnet[C] with AddSubtractable[C] with HexCompatible[C]
    with ComparableWith[NumericCol[_]] with LogicalOps with ArithmeticOps[C]

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

  /**
    * Marks types that can be checked on empty/nonempty and length (atleast collections and strings)
    */
  sealed trait EmptyNonEmptyCol[C] extends Magnet[C]

//  implicit def emptyNonEmptyFromStringCol[T <: TableColumn[String]](s: T) =
//    new EmptyNonEmptyCol[String] {
//      override val column: TableColumn[String] = s
//    }

  implicit def emptyNonEmptyFromIterableCol[Elem, Collection[B] <: Iterable[B], ColType[A] <: TableColumn[A]](s: ColType[Collection[Elem]]) =
    new EmptyNonEmptyCol[Collection[Elem]] {
      override val column: TableColumn[Collection[Elem]] = s
    }

//  implicit def emptyNonEmptyFromString[T <: String : QueryValue](s: T) =
//    new EmptyNonEmptyCol[String] {
//      override val column = Const(s)
//    }

  implicit def emptyNonEmptyFromIterable[T <: Iterable[_] : QueryValue](s: T) =
    new EmptyNonEmptyCol[T] {
      override val column = Const(s)
    }

}
