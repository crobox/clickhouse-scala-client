package com.crobox.clickhouse.dsl.column

import java.util.UUID
import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType
import com.crobox.clickhouse.dsl.{Const, EmptyColumn, ExpressionColumn, OperationalQuery, Table, TableColumn}
import org.joda.time.{DateTime, LocalDate}
import scala.language.implicitConversions


trait Magnets {
  self: ArithmeticFunctions
    //with ComparisonFunctions
    with LogicalFunctions
    //with TypeCastFunctions
    with StringFunctions
    with EmptyFunctions
    with StringSearchFunctions
    with ScalaBooleanFunctions
    with ScalaStringFunctions
    with InFunctions =>

  /**
   * Magnet pattern
   *
   * The pattern provides implicit conversion to wrapper classes,
   * this allows the DSL to accept multiple compatible column types in a single function.
   */
  trait Magnet[+C] {
    val column: TableColumn[C]
  }

  case class ComparisonColumn(left: Magnet[_], operator: String, right: Magnet[_]) extends ExpressionColumn[Boolean](EmptyColumn)

  trait ComparableWith[M <: Magnet[_]] {
    self: Magnet[_] =>
    def <(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "<", other)
    def >(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, ">", other)
    def <>(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "!=", other)
    def isEq(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "=", other)
    def notEq(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "!=", other)
    def ===(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "=", other)
    def !==(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "!=", other)
    def <=(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, "<=", other)
    def >=(other: M): ExpressionColumn[Boolean] = ComparisonColumn(self, ">=", other)
  }

  def _equals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1, "=", col2)
  def notEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1, "!=", col2)
  def less(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1, "<", col2)
  def greater(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1, ">", col2)
  def lessOrEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1, "<=", col2)
  def greaterOrEquals(col1: ConstOrColMagnet[_], col2: ConstOrColMagnet[_]): ExpressionColumn[Boolean] = ComparisonColumn(col1, ">=", col2)


  abstract class TypeCastColumn[V](val targetColumn: ConstOrColMagnet[_])
    extends ExpressionColumn[V](targetColumn.column)

  case class Reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable)
    extends TypeCastColumn[V](typeCastColumn.targetColumn)

  //Tagging of compatible
  sealed trait Reinterpretable

  // @todo Unsigned types are basically not supported. For now keep the same as signed types
  case class UInt8(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Option[Byte] = None,
                   orNull: Boolean = false)
    extends TypeCastColumn[Byte](tableColumn)
      with Reinterpretable

  case class UInt16(tableColumn: ConstOrColMagnet[_],
                    orZero: Boolean = false,
                    orDefault: Option[Short] = None,
                    orNull: Boolean = false)
    extends TypeCastColumn[Short](tableColumn)
      with Reinterpretable

  case class UInt32(tableColumn: ConstOrColMagnet[_],
                    orZero: Boolean = false,
                    orDefault: Option[Int] = None,
                    orNull: Boolean = false)
    extends TypeCastColumn[Int](tableColumn)
      with Reinterpretable

  case class UInt64(tableColumn: ConstOrColMagnet[_],
                    orZero: Boolean = false,
                    orDefault: Option[Long] = None,
                    orNull: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Int8(tableColumn: ConstOrColMagnet[_],
                  orZero: Boolean = false,
                  orDefault: Option[Byte] = None,
                  orNull: Boolean = false)
    extends TypeCastColumn[Byte](tableColumn)
      with Reinterpretable

  case class Int16(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Option[Short] = None,
                   orNull: Boolean = false)
    extends TypeCastColumn[Short](tableColumn)
      with Reinterpretable

  case class Int32(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Option[Int] = None,
                   orNull: Boolean = false)
    extends TypeCastColumn[Int](tableColumn)
      with Reinterpretable

  case class Int64(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Option[Long] = None,
                   orNull: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Float32(tableColumn: ConstOrColMagnet[_],
                     orZero: Boolean = false,
                     orDefault: Option[Float] = None,
                     orNull: Boolean = false)
    extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable

  case class Float64(tableColumn: ConstOrColMagnet[_],
                     orZero: Boolean = false,
                     orDefault: Option[Double] = None,
                     orNull: Boolean = false)
    extends TypeCastColumn[Double](tableColumn)
      with Reinterpretable

  case class Uuid(tableColumn: ConstOrColMagnet[_],
                  orZero: Boolean = false,
                  orDefault: Option[Uuid] = None,
                  orNull: Boolean = false)
    extends TypeCastColumn[java.util.UUID](tableColumn)
      with Reinterpretable

  case class DateRep(tableColumn: ConstOrColMagnet[_],
                     orZero: Boolean = false,
                     orDefault: Option[DateTime] = None,
                     orNull: Boolean = false)
    extends TypeCastColumn[org.joda.time.LocalDate](tableColumn)
      with Reinterpretable

  case class DateTimeRep(tableColumn: ConstOrColMagnet[_],
                         orZero: Boolean = false,
                         orDefault: Option[org.joda.time.DateTime] = None,
                         orNull: Boolean = false)
    extends TypeCastColumn[org.joda.time.DateTime](tableColumn)
      with Reinterpretable

  case class StringRep(tableColumn: ConstOrColMagnet[_])
    extends TypeCastColumn[String](tableColumn)
      with Reinterpretable

  case class FixedString(tableColumn: ConstOrColMagnet[_], n: Int) extends TypeCastColumn[String](tableColumn)

  case class StringCutToZero(tableColumn: ConstOrColMagnet[_]) extends TypeCastColumn[String](tableColumn)

  case class Cast[T](tableColumn: ConstOrColMagnet[_], simpleColumnType: SimpleColumnType)
    extends TypeCastColumn[T](tableColumn)
      with ConstOrColMagnet[T] {
    override val column: TableColumn[T] = this
  }

  sealed trait CastOutBind[I, O]

  implicit object UInt8CastOutBind extends CastOutBind[ColumnType.UInt8.type, Byte]

  implicit object UInt16CastOutBind extends CastOutBind[ColumnType.UInt16.type, Short]

  implicit object UInt32CastOutBind extends CastOutBind[ColumnType.UInt32.type, Int]

  implicit object UInt64CastOutBind extends CastOutBind[ColumnType.UInt64.type, Long]

  implicit object Int8CastOutBind extends CastOutBind[ColumnType.Int8.type, Byte]

  implicit object Int16CastOutBind extends CastOutBind[ColumnType.Int16.type, Short]

  implicit object Int32CastOutBind extends CastOutBind[ColumnType.Int32.type, Int]

  implicit object Int64CastOutBind extends CastOutBind[ColumnType.Int64.type, Long]

  implicit object Float32CastOutBind extends CastOutBind[ColumnType.Float32.type, Float]

  implicit object Float64CastOutBind extends CastOutBind[ColumnType.Float64.type, Double]

  implicit object StringCastOutBind extends CastOutBind[ColumnType.String.type, Int]

  implicit object FixedStringCastOutBind extends CastOutBind[ColumnType.FixedString.type, Int]

  implicit object DateCastOutBind extends CastOutBind[ColumnType.Date.type, Int]

  implicit object DateTimeCastOutBind extends CastOutBind[ColumnType.DateTime.type, Int]

  def toUInt8(tableColumn: ConstOrColMagnet[_]): UInt8 = UInt8(tableColumn)

  def toUInt8OrDefault(tableColumn: ConstOrColMagnet[_], value: Byte): UInt8 =
    UInt8(tableColumn, orDefault = Option(value))

  def toUInt8OrNull(tableColumn: ConstOrColMagnet[_]): UInt8 = UInt8(tableColumn, orNull = true)

  def toUInt8OrZero(tableColumn: ConstOrColMagnet[_]): UInt8 = UInt8(tableColumn, orZero = true)

  def toUInt16(tableColumn: ConstOrColMagnet[_]): UInt16 = UInt16(tableColumn)

  def toUInt16OrDefault(tableColumn: ConstOrColMagnet[_], value: Short): UInt16 =
    UInt16(tableColumn, orDefault = Option(value))

  def toUInt16OrNull(tableColumn: ConstOrColMagnet[_]): UInt16 = UInt16(tableColumn, orNull = true)

  def toUInt16OrZero(tableColumn: ConstOrColMagnet[_]): UInt16 = UInt16(tableColumn, orZero = true)

  def toUInt32(tableColumn: ConstOrColMagnet[_]): UInt32 = UInt32(tableColumn)

  def toUInt32OrDefault(tableColumn: ConstOrColMagnet[_], value: Int): UInt32 =
    UInt32(tableColumn, orDefault = Option(value))

  def toUInt32OrNull(tableColumn: ConstOrColMagnet[_]): UInt32 = UInt32(tableColumn, orNull = true)

  def toUInt32OrZero(tableColumn: ConstOrColMagnet[_]): UInt32 = UInt32(tableColumn, orZero = true)

  def toUInt64(tableColumn: ConstOrColMagnet[_]): UInt64 = UInt64(tableColumn)

  def toUInt64rDefault(tableColumn: ConstOrColMagnet[_], value: Long): UInt64 =
    UInt64(tableColumn, orDefault = Option(value))

  def toUInt64rNull(tableColumn: ConstOrColMagnet[_]): UInt64 = UInt64(tableColumn, orNull = true)

  def toUInt64OrZero(tableColumn: ConstOrColMagnet[_]): UInt64 = UInt64(tableColumn, orZero = true)

  def toInt8(tableColumn: ConstOrColMagnet[_]): Int8 = Int8(tableColumn)

  def toInt8OrDefault(tableColumn: ConstOrColMagnet[_], value: Byte): Int8 =
    Int8(tableColumn, orDefault = Option(value))

  def toInt8OrNull(tableColumn: ConstOrColMagnet[_]): Int8 = Int8(tableColumn, orNull = true)

  def toInt8OrZero(tableColumn: ConstOrColMagnet[_]): Int8 = Int8(tableColumn, orZero = true)

  def toInt16(tableColumn: ConstOrColMagnet[_]): Int16 = Int16(tableColumn)

  def toInt16OrDefault(tableColumn: ConstOrColMagnet[_], value: Short): Int16 =
    Int16(tableColumn, orDefault = Option(value))

  def toInt16OrNull(tableColumn: ConstOrColMagnet[_]): Int16 = Int16(tableColumn, orNull = true)

  def toInt16OrZero(tableColumn: ConstOrColMagnet[_]): Int16 = Int16(tableColumn, orZero = true)

  def toInt32(tableColumn: ConstOrColMagnet[_]): Int32 = Int32(tableColumn)

  def toInt32OrDefault(tableColumn: ConstOrColMagnet[_], value: Int): Int32 =
    Int32(tableColumn, orDefault = Option(value))

  def toInt32OrNull(tableColumn: ConstOrColMagnet[_]): Int32 = Int32(tableColumn, orNull = true)

  def toInt32OrZero(tableColumn: ConstOrColMagnet[_]): Int32 = Int32(tableColumn, orZero = true)

  def toInt64(tableColumn: ConstOrColMagnet[_]): Int64 = Int64(tableColumn)

  def toInt64OrDefault(tableColumn: ConstOrColMagnet[_], value: Long): Int64 =
    Int64(tableColumn, orDefault = Option(value))

  def toInt64OrNull(tableColumn: ConstOrColMagnet[_]): Int64 = Int64(tableColumn, orNull = true)

  def toInt64OrZero(tableColumn: ConstOrColMagnet[_]): Int64 = Int64(tableColumn, orZero = true)

  def toFloat32(tableColumn: ConstOrColMagnet[_]): Float32 = Float32(tableColumn)

  def toFloat32OrDefault(tableColumn: ConstOrColMagnet[_], value: Float): Float32 =
    Float32(tableColumn, orDefault = Option(value))

  def toFloat32OrNull(tableColumn: ConstOrColMagnet[_]): Float32 = Float32(tableColumn, orNull = true)

  def toFloat32OrZero(tableColumn: ConstOrColMagnet[_]): Float32 = Float32(tableColumn, orZero = true)

  def toFloat64(tableColumn: ConstOrColMagnet[_]): Float64 = Float64(tableColumn)

  def toFloat64OrDefault(tableColumn: ConstOrColMagnet[_], value: Double): Float64 =
    Float64(tableColumn, orDefault = Option(value))

  def toFloat64OrNull(tableColumn: ConstOrColMagnet[_]): Float64 = Float64(tableColumn, orNull = true)

  def toFloat64OrZero(tableColumn: ConstOrColMagnet[_]): Float64 = Float64(tableColumn, orZero = true)

  def toDate(tableColumn: ConstOrColMagnet[_]): DateRep = DateRep(tableColumn)

  def toDateOrDefault(tableColumn: ConstOrColMagnet[_], value: DateTime): DateRep =
    DateRep(tableColumn, orDefault = Option(value))

  def toDateOrNull(tableColumn: ConstOrColMagnet[_]): DateRep = DateRep(tableColumn, orNull = true)

  def toDateOrZero(tableColumn: ConstOrColMagnet[_]): DateRep = DateRep(tableColumn, orZero = true)

  def toDateTime(tableColumn: ConstOrColMagnet[_]): DateTimeRep = DateTimeRep(tableColumn)

  def toDateTimeOrDefault(tableColumn: ConstOrColMagnet[_], value: DateTime): DateTimeRep =
    DateTimeRep(tableColumn, orDefault = Option(value))

  def toDateTimeOrNull(tableColumn: ConstOrColMagnet[_]): DateTimeRep = DateTimeRep(tableColumn, orNull = true)

  def toDateTimeOrZero(tableColumn: ConstOrColMagnet[_]): DateTimeRep = DateTimeRep(tableColumn, orZero = true)

  def toStringRep(tableColumn: ConstOrColMagnet[_]): StringRep = StringRep(tableColumn)

  def toFixedString(tableColumn: ConstOrColMagnet[_], n: Int): FixedString = FixedString(tableColumn, n)

  def toStringCutToZero(tableColumn: ConstOrColMagnet[_]): StringCutToZero = StringCutToZero(tableColumn)

  def toUUID(tableColumn: ConstOrColMagnet[_]): Uuid = Uuid(tableColumn)

  def toUUIDOrDefault(tableColumn: ConstOrColMagnet[_], value: Uuid): Uuid =
    Uuid(tableColumn, orDefault = Option(value))

  def toUUIDOrNull(tableColumn: ConstOrColMagnet[_]): Uuid = Uuid(tableColumn, orNull = true)

  def toUUIDOrZero(tableColumn: ConstOrColMagnet[_]): Uuid = Uuid(tableColumn, orZero = true)

  def reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable): Reinterpret[V] =
    Reinterpret[V](typeCastColumn)

  def cast[T <: SimpleColumnType, O](tableColumn: ConstOrColMagnet[_], simpleColumnType: T)(
    implicit castOut: CastOutBind[T, O]
  ): Cast[O] = Cast[O](tableColumn, simpleColumnType)

  /**
   * Any constant or column.
   * Sidenote: The current implementation doesn't represent collections.
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
      override val column                          = EmptyColumn
      override val query: Option[OperationalQuery] = Some(s)
    }

  implicit def InFuncRHMagnetFromTable(s: Table): InFuncRHMagnet =
    new InFuncRHMagnet {
      override val column                  = EmptyColumn
      override val tableRef: Option[Table] = Some(s)
    }

  /**
   * Represents any collection
   */
  sealed trait ArrayColMagnet[+C] extends Magnet[C]

  implicit def arrayColMagnetFromIterableConst[T: QueryValue](s: scala.Iterable[T]): ArrayColMagnet[scala.Iterable[T]] =
    new ArrayColMagnet[scala.Iterable[T]] {
      val qvForIterable = QueryValueFormats.queryValueToSeq(implicitly[QueryValue[T]])
      override val column = Const(s)(qvForIterable)
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
      override val column = s
    }

  implicit def ddtFromDateTimeCol[T <: TableColumn[DateTime]](s: T): DateOrDateTime[DateTime] =
    new DateOrDateTime[DateTime] {
      override val column = s
    }

  implicit def ddtFromDate[T <: LocalDate: QueryValue](s: T): DateOrDateTime[LocalDate] =
    new DateOrDateTime[LocalDate] {
      override val column = toDate(s)
    }

  implicit def ddtFromDateTime[T <: DateTime: QueryValue](s: T): DateOrDateTime[DateTime] =
    new DateOrDateTime[DateTime] {
      override val column = toDateTime(s)
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
      override val column = Const(s)
    }

  implicit def numericFromInt[T <: Int: QueryValue](s: T): NumericCol[Int] =
    new NumericCol[Int] {
      override val column = Const(s)
    }

  implicit def numericFromDouble[T <: Double: QueryValue](s: T): NumericCol[Double] =
    new NumericCol[Double] {
      override val column = Const(s)
    }

  implicit def numericFromFloat[T <: Float: QueryValue](s: T): NumericCol[Float] =
    new NumericCol[Float] {
      override val column = Const(s)
    }

  implicit def numericFromBigInt[T <: BigInt: QueryValue](s: T): NumericCol[BigInt] =
    new NumericCol[BigInt] {
      override val column = Const(s)
    }

  implicit def numericFromBigDecimal[T <: BigDecimal: QueryValue](s: T): NumericCol[BigDecimal] =
    new NumericCol[BigDecimal] {
      override val column = Const(s)
    }

  implicit def numericFromBoolean[T <: Boolean: QueryValue](s: T): NumericCol[Boolean] =
    new NumericCol[Boolean] {
      override val column = Const(s)
    }

  implicit def numericFromLongCol[T <: TableColumn[Long]](s: T): NumericCol[Long] =
    new NumericCol[Long] {
      override val column = s
    }

  implicit def numericFromIntCol[T <: TableColumn[Int]](s: T): NumericCol[Int] =
    new NumericCol[Int] {
      override val column = s
    }

  implicit def numericFromDoubleCol[T <: TableColumn[Double]](s: T): NumericCol[Double] =
    new NumericCol[Double] {
      override val column = s
    }

  implicit def numericFromFloatCol[T <: TableColumn[Float]](s: T): NumericCol[Float] =
    new NumericCol[Float] {
      override val column = s
    }

  implicit def numericFromBigIntCol[T <: TableColumn[BigInt]](s: T): NumericCol[BigInt] =
    new NumericCol[BigInt] {
      override val column = s
    }

  implicit def numericFromBigDecimalCol[T <: TableColumn[BigDecimal]](s: T): NumericCol[BigDecimal] =
    new NumericCol[BigDecimal] {
      override val column = s
    }

  implicit def numericFromBooleanCol[T <: TableColumn[Boolean]](s: T): NumericCol[Boolean] =
    new NumericCol[Boolean] {
      override val column = s
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
      override val column = Const(s)
    }

}
