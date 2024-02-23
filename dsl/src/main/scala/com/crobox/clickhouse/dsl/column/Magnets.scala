package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats.BooleanQueryValue
import com.crobox.clickhouse.dsl.{Const, EmptyColumn, OperationalQuery, Table, TableColumn}
import org.joda.time.{DateTime, LocalDate}

import java.util.UUID

trait Magnets {
  self: ArithmeticFunctions
    with ComparisonFunctions
    with LogicalFunctions
    with TypeCastFunctions
    with StringFunctions
    with EmptyFunctions
    with StringSearchFunctions
    with ScalaBooleanFunctions
    with ScalaStringFunctions
    with InFunctions =>

  /**
   * Magnet pattern
   *
   * The pattern provides implicit conversion to wrapper classes, this allows the DSL to accept multiple compatible
   * column types in a single function.
   */
  trait Magnet[+C] {
    val column: TableColumn[C]
  }

  /**
   * Any constant or column. Sidenote: The current implementation doesn't represent collections.
   */
  trait ConstOrColMagnet[+C] extends Magnet[C] with ScalaBooleanFunctionOps with InOps

  implicit def constOrColMagnetFromCol[C](s: TableColumn[C]): ConstOrColMagnet[C] =
    new ConstOrColMagnet[C] {
      override val column: TableColumn[C] = s
    }

  implicit def constOrColMagnetFromConst[T: QueryValue](s: T): ConstOrColMagnet[T] =
    new ConstOrColMagnet[T] {
      override val column: TableColumn[T] = Const(s)
    }

  /**
   * Represents any accepted type for the right hand argument of the IN operators (tuple, table or Qry)
   */
  sealed trait InFuncRHMagnet extends Magnet[Nothing] {
    val query: Option[OperationalQuery] = None
    val tableRef: Option[Table]         = None

    val isEmptyCollection: Boolean = false
  }

  implicit def InFuncRHMagnetFromIterable[T: QueryValue](s: Iterable[T]): InFuncRHMagnet =
    new InFuncRHMagnet {
      val qvT                               = implicitly[QueryValue[T]]
      val sConsts: Seq[ConstOrColMagnet[T]] = s.map(col => constOrColMagnetFromConst(col)(qvT)).toSeq

      override val column: Tuple              = tuple(sConsts: _*)
      override val isEmptyCollection: Boolean = column.coln.isEmpty
    }

  implicit def InFuncRHMagnetFromTuple(s: Tuple): InFuncRHMagnet =
    new InFuncRHMagnet {
      override val column: Tuple              = s
      override val isEmptyCollection: Boolean = column.coln.isEmpty
    }

  implicit def InFuncRHMagnetFromQuery(s: OperationalQuery): InFuncRHMagnet =
    new InFuncRHMagnet {
      override val column: TableColumn[Nothing]    = EmptyColumn
      override val query: Option[OperationalQuery] = Some(s)
    }

  implicit def InFuncRHMagnetFromTable(s: Table): InFuncRHMagnet =
    new InFuncRHMagnet {
      override val column: TableColumn[Nothing] = EmptyColumn
      override val tableRef: Option[Table]      = Some(s)
    }

  /**
   * Represents any collection
   */
  sealed trait ArrayColMagnet[+C] extends Magnet[C]

  implicit def arrayColMagnetFromIterableConst[T: QueryValue](s: scala.Iterable[T]): ArrayColMagnet[scala.Iterable[T]] =
    new ArrayColMagnet[scala.Iterable[T]] {
      val qvForIterable                       = QueryValueFormats.queryValueToSeq(implicitly[QueryValue[T]])
      override val column: Const[Iterable[T]] = Const(s)(qvForIterable)
    }

  implicit def arrayColMagnetFromIterableCol[C](s: TableColumn[scala.Iterable[C]]): ArrayColMagnet[scala.Iterable[C]] =
    new ArrayColMagnet[scala.Iterable[C]] {
      override val column = s
    }

  /**
   * UUID-like columns
   */
  trait UUIDColMagnet[C]
      extends Magnet[C]
      with HexCompatible[C]
      with ComparableWith[UUIDColMagnet[_]]
      with EmptyOps[C]
      with EmptyNonEmptyCol[C]

  /**
   * String-like columns
   */
  trait StringColMagnet[C]
      extends Magnet[C]
      with HexCompatible[C]
      with ComparableWith[StringColMagnet[_]]
      with ScalaStringFunctionOps
      with EmptyOps[C]
      with StringOps
      with StringSearchOps
      with EmptyNonEmptyCol[C]

  implicit def stringColMagnetFromString[T <: String: QueryValue](s: T): StringColMagnet[String] =
    new StringColMagnet[String] {
      override val column: TableColumn[String] = Const(s)
    }

  implicit def stringColMagnetFromStringCol[T <: TableColumn[String]](s: T): StringColMagnet[String] =
    new StringColMagnet[String] {
      override val column: TableColumn[String] = s
    }

  implicit def stringColMagnetFromUUID[T <: UUID: QueryValue](s: T): StringColMagnet[UUID] =
    new StringColMagnet[UUID] {
      override val column: TableColumn[UUID] = Const(s)
    }

  implicit def stringColMagnetFromUUIDCol[T <: TableColumn[UUID]](s: T): StringColMagnet[UUID] =
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

  implicit def ddtFromDateCol[T <: TableColumn[LocalDate]](s: T): DateOrDateTime[LocalDate] =
    new DateOrDateTime[LocalDate] {
      override val column: T = s
    }

  implicit def ddtFromDateTimeCol[T <: TableColumn[DateTime]](s: T): DateOrDateTime[DateTime] =
    new DateOrDateTime[DateTime] {
      override val column: T = s
    }

  implicit def ddtFromDate[T <: LocalDate: QueryValue](s: T): DateOrDateTime[LocalDate] =
    new DateOrDateTime[LocalDate] {
      override val column: DateRep = toDate(s)
    }

  implicit def ddtFromDateTime[T <: DateTime: QueryValue](s: T): DateOrDateTime[DateTime] =
    new DateOrDateTime[DateTime] {
      override val column: DateTimeRep = toDateTime(s)
    }

  sealed trait LogicalOpsMagnet extends LogicalOps {
    val asOption: Option[TableColumn[Boolean]]

    def isConstTrue: Boolean = asOption match {
      case Some(Const(el: Boolean)) => el
      case _                        => false
    }

    def isConstFalse: Boolean = asOption match {
      case Some(Const(false)) => true
      case _                  => false
    }
  }

  implicit def logicalOpsMagnetFromOptionCol(s: Option[TableColumn[Boolean]]): LogicalOpsMagnet =
    new LogicalOpsMagnet {
      override val asOption: Option[TableColumn[Boolean]] = s
    }

  implicit def logicalOpsMagnetFromOptionConst(s: Option[Boolean]): LogicalOpsMagnet =
    new LogicalOpsMagnet {
      override val asOption: Option[TableColumn[Boolean]] = s.map(Const(_))
    }

  implicit def logicalOpsMagnetFromNone(s: Option[Nothing]): LogicalOpsMagnet =
    new LogicalOpsMagnet {
      override val asOption: Option[TableColumn[Boolean]] = None
    }

  implicit def logicalOpsMagnetFromBoolean(s: Boolean): LogicalOpsMagnet =
    new LogicalOpsMagnet {
      override val asOption: Option[TableColumn[Boolean]] = Some(Const(s))
    }

  implicit def logicalOpsMagnetFromBooleanCol(s: TableColumn[Boolean]): LogicalOpsMagnet =
    new LogicalOpsMagnet {
      override val asOption: Option[TableColumn[Boolean]] = Some(s)
    }

  /**
   * Type that is expected by functions that shall then add or subtract from this value.
   *
   * These are not just numerics, but f.i. also dates / times.
   */
  sealed trait AddSubtractable[C] extends Magnet[C] with AddSubtractOps[C]

  trait NumericCol[C]
      extends Magnet[C]
      with AddSubtractable[C]
      with HexCompatible[C]
      with ComparableWith[NumericCol[_]]
      with ArithmeticOps[C]

  implicit def numericFromLong[T <: Long: QueryValue](s: T): NumericCol[Long] =
    new NumericCol[Long] {
      override val column: Const[T] = Const(s)
    }

  implicit def numericFromInt[T <: Int: QueryValue](s: T): NumericCol[Int] =
    new NumericCol[Int] {
      override val column: Const[T] = Const(s)
    }

  implicit def numericFromDouble[T <: Double: QueryValue](s: T): NumericCol[Double] =
    new NumericCol[Double] {
      override val column: Const[T] = Const(s)
    }

  implicit def numericFromFloat[T <: Float: QueryValue](s: T): NumericCol[Float] =
    new NumericCol[Float] {
      override val column: Const[T] = Const(s)
    }

  implicit def numericFromBigInt[T <: BigInt: QueryValue](s: T): NumericCol[BigInt] =
    new NumericCol[BigInt] {
      override val column: Const[T] = Const(s)
    }

  implicit def numericFromBigDecimal[T <: BigDecimal: QueryValue](s: T): NumericCol[BigDecimal] =
    new NumericCol[BigDecimal] {
      override val column: Const[T] = Const(s)
    }

  implicit def numericFromBoolean[T <: Boolean: QueryValue](s: T): NumericCol[Boolean] =
    new NumericCol[Boolean] {
      override val column: Const[T] = Const(s)
    }

  implicit def numericFromLongCol[T <: TableColumn[Long]](s: T): NumericCol[Long] =
    new NumericCol[Long] {
      override val column: T = s
    }

  implicit def numericFromIntCol[T <: TableColumn[Int]](s: T): NumericCol[Int] =
    new NumericCol[Int] {
      override val column: T = s
    }

  implicit def numericFromDoubleCol[T <: TableColumn[Double]](s: T): NumericCol[Double] =
    new NumericCol[Double] {
      override val column: T = s
    }

  implicit def numericFromFloatCol[T <: TableColumn[Float]](s: T): NumericCol[Float] =
    new NumericCol[Float] {
      override val column: T = s
    }

  implicit def numericFromBigIntCol[T <: TableColumn[BigInt]](s: T): NumericCol[BigInt] =
    new NumericCol[BigInt] {
      override val column: T = s
    }

  implicit def numericFromBigDecimalCol[T <: TableColumn[BigDecimal]](s: T): NumericCol[BigDecimal] =
    new NumericCol[BigDecimal] {
      override val column: T = s
    }

  implicit def numericFromBooleanCol[T <: TableColumn[Boolean]](s: T): NumericCol[Boolean] =
    new NumericCol[Boolean] {
      override val column: T = s
    }

  /**
   * Marks types that can be checked on empty/nonempty and length (at least collections and strings)
   */
  sealed trait EmptyNonEmptyCol[C] extends Magnet[C]

  implicit def emptyNonEmptyFromIterableCol[Elem, Collection[B] <: Iterable[B], ColType[A] <: TableColumn[A]](
      s: ColType[Collection[Elem]]
  ): EmptyNonEmptyCol[Collection[Elem]] =
    new EmptyNonEmptyCol[Collection[Elem]] {
      override val column: TableColumn[Collection[Elem]] = s
    }

  implicit def emptyNonEmptyFromIterable[T <: Iterable[_]: QueryValue](s: T): EmptyNonEmptyCol[T] =
    new EmptyNonEmptyCol[T] {
      override val column: Const[T] = Const(s)
    }

}
