package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType
import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}

trait TypeCastFunctions { self: Magnets =>

  abstract class TypeCastColumn[V](val targetColumn: ConstOrColMagnet[_])
      extends ExpressionColumn[V](targetColumn.column)

  case class Reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable)
      extends TypeCastColumn[V](typeCastColumn.targetColumn)

  //Tagging of compatible
  sealed trait Reinterpretable

  // @todo Unsigned types are basically not supported. For now keep the same as signed types
  case class UInt8(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Boolean = false,
                   orNull: Boolean = false)
      extends TypeCastColumn[Byte](tableColumn)
      with Reinterpretable
  case class UInt16(tableColumn: ConstOrColMagnet[_],
                    orZero: Boolean = false,
                    orDefault: Boolean = false,
                    orNull: Boolean = false)
      extends TypeCastColumn[Short](tableColumn)
      with Reinterpretable
  case class UInt32(tableColumn: ConstOrColMagnet[_],
                    orZero: Boolean = false,
                    orDefault: Boolean = false,
                    orNull: Boolean = false)
      extends TypeCastColumn[Int](tableColumn)
      with Reinterpretable
  case class UInt64(tableColumn: ConstOrColMagnet[_],
                    orZero: Boolean = false,
                    orDefault: Boolean = false,
                    orNull: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Int8(tableColumn: ConstOrColMagnet[_],
                  orZero: Boolean = false,
                  orDefault: Boolean = false,
                  orNull: Boolean = false)
      extends TypeCastColumn[Byte](tableColumn)
      with Reinterpretable
  case class Int16(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Boolean = false,
                   orNull: Boolean = false)
      extends TypeCastColumn[Short](tableColumn)
      with Reinterpretable
  case class Int32(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Boolean = false,
                   orNull: Boolean = false)
      extends TypeCastColumn[Int](tableColumn)
      with Reinterpretable
  case class Int64(tableColumn: ConstOrColMagnet[_],
                   orZero: Boolean = false,
                   orDefault: Boolean = false,
                   orNull: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Float32(tableColumn: ConstOrColMagnet[_],
                     orZero: Boolean = false,
                     orDefault: Boolean = false,
                     orNull: Boolean = false)
      extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable
  case class Float64(tableColumn: ConstOrColMagnet[_],
                     orZero: Boolean = false,
                     orDefault: Boolean = false,
                     orNull: Boolean = false)
      extends TypeCastColumn[Double](tableColumn)
      with Reinterpretable

  case class Uuid(tableColumn: ConstOrColMagnet[_],
                  orZero: Boolean = false,
                  orNull: Boolean = false,
                  orDefault: Boolean = false)
      extends TypeCastColumn[java.util.UUID](tableColumn)
      with Reinterpretable

  case class DateRep(tableColumn: ConstOrColMagnet[_])
      extends TypeCastColumn[org.joda.time.LocalDate](tableColumn)
      with Reinterpretable
  case class DateTimeRep(tableColumn: ConstOrColMagnet[_],
                         orZero: Boolean = false,
                         orNull: Boolean = false,
                         orDefault: Boolean = false)
      extends TypeCastColumn[org.joda.time.DateTime](tableColumn)
      with Reinterpretable

  case class StringRep(tableColumn: ConstOrColMagnet[_])
      extends TypeCastColumn[String](tableColumn)
      with Reinterpretable
  case class FixedString(tableColumn: ConstOrColMagnet[_], n: Int) extends TypeCastColumn[String](tableColumn)
  case class StringCutToZero(tableColumn: ConstOrColMagnet[_])     extends TypeCastColumn[String](tableColumn)

  case class Cast[T](tableColumn: ConstOrColMagnet[_], simpleColumnType: SimpleColumnType)
      extends TypeCastColumn[T](tableColumn)
      with ConstOrColMagnet[T] {
    override val column: TableColumn[T] = this
  }

  sealed trait CastOutBind[I, O]
  implicit object UInt8CastOutBind       extends CastOutBind[ColumnType.UInt8.type, Byte]
  implicit object UInt16CastOutBind      extends CastOutBind[ColumnType.UInt16.type, Short]
  implicit object UInt32CastOutBind      extends CastOutBind[ColumnType.UInt32.type, Int]
  implicit object UInt64CastOutBind      extends CastOutBind[ColumnType.UInt64.type, Long]
  implicit object Int8CastOutBind        extends CastOutBind[ColumnType.Int8.type, Byte]
  implicit object Int16CastOutBind       extends CastOutBind[ColumnType.Int16.type, Short]
  implicit object Int32CastOutBind       extends CastOutBind[ColumnType.Int32.type, Int]
  implicit object Int64CastOutBind       extends CastOutBind[ColumnType.Int64.type, Long]
  implicit object Float32CastOutBind     extends CastOutBind[ColumnType.Float32.type, Float]
  implicit object Float64CastOutBind     extends CastOutBind[ColumnType.Float64.type, Double]
  implicit object StringCastOutBind      extends CastOutBind[ColumnType.String.type, Int]
  implicit object FixedStringCastOutBind extends CastOutBind[ColumnType.FixedString.type, Int]
  implicit object DateCastOutBind        extends CastOutBind[ColumnType.Date.type, Int]
  implicit object DateTimeCastOutBind    extends CastOutBind[ColumnType.DateTime.type, Int]

  def toUInt8(tableColumn: ConstOrColMagnet[_]): UInt8            = UInt8(tableColumn)
  def toUInt8OrDefault(tableColumn: ConstOrColMagnet[_]): UInt8   = UInt8(tableColumn, orDefault = true)
  def toUInt8OrNull(tableColumn: ConstOrColMagnet[_]): UInt8      = UInt8(tableColumn, orNull = true)
  def toUInt8OrZero(tableColumn: ConstOrColMagnet[_]): UInt8      = UInt8(tableColumn, orZero = true)
  def toUInt16(tableColumn: ConstOrColMagnet[_]): UInt16          = UInt16(tableColumn)
  def toUInt16OrDefault(tableColumn: ConstOrColMagnet[_]): UInt16 = UInt16(tableColumn, orDefault = true)
  def toUInt16OrNull(tableColumn: ConstOrColMagnet[_]): UInt16    = UInt16(tableColumn, orNull = true)
  def toUInt16OrZero(tableColumn: ConstOrColMagnet[_]): UInt16    = UInt16(tableColumn, orZero = true)
  def toUInt32(tableColumn: ConstOrColMagnet[_]): UInt32          = UInt32(tableColumn)
  def toUInt32OrDefault(tableColumn: ConstOrColMagnet[_]): UInt32 = UInt32(tableColumn, orDefault = true)
  def toUInt32OrNull(tableColumn: ConstOrColMagnet[_]): UInt32    = UInt32(tableColumn, orNull = true)
  def toUInt32OrZero(tableColumn: ConstOrColMagnet[_]): UInt32    = UInt32(tableColumn, orZero = true)
  def toUInt64(tableColumn: ConstOrColMagnet[_]): UInt64          = UInt64(tableColumn)
  def toUInt64rDefault(tableColumn: ConstOrColMagnet[_]): UInt64  = UInt64(tableColumn, orDefault = true)
  def toUInt64rNull(tableColumn: ConstOrColMagnet[_]): UInt64     = UInt64(tableColumn, orNull = true)
  def toUInt64OrZero(tableColumn: ConstOrColMagnet[_]): UInt64    = UInt64(tableColumn, orZero = true)

  def toInt8(tableColumn: ConstOrColMagnet[_]): Int8            = Int8(tableColumn)
  def toInt8OrDefault(tableColumn: ConstOrColMagnet[_]): Int8   = Int8(tableColumn, orDefault = true)
  def toInt8OrNull(tableColumn: ConstOrColMagnet[_]): Int8      = Int8(tableColumn, orNull = true)
  def toInt8OrZero(tableColumn: ConstOrColMagnet[_]): Int8      = Int8(tableColumn, orZero = true)
  def toInt16(tableColumn: ConstOrColMagnet[_]): Int16          = Int16(tableColumn)
  def toInt16OrDefault(tableColumn: ConstOrColMagnet[_]): Int16 = Int16(tableColumn, orDefault = true)
  def toInt16OrNull(tableColumn: ConstOrColMagnet[_]): Int16    = Int16(tableColumn, orNull = true)
  def toInt16OrZero(tableColumn: ConstOrColMagnet[_]): Int16    = Int16(tableColumn, orZero = true)
  def toInt32(tableColumn: ConstOrColMagnet[_]): Int32          = Int32(tableColumn)
  def toInt32OrDefault(tableColumn: ConstOrColMagnet[_]): Int32 = Int32(tableColumn, orDefault = true)
  def toInt32OrNull(tableColumn: ConstOrColMagnet[_]): Int32    = Int32(tableColumn, orNull = true)
  def toInt32OrZero(tableColumn: ConstOrColMagnet[_]): Int32    = Int32(tableColumn, orZero = true)
  def toInt64(tableColumn: ConstOrColMagnet[_]): Int64          = Int64(tableColumn)
  def toInt64OrDefault(tableColumn: ConstOrColMagnet[_]): Int64 = Int64(tableColumn, orDefault = true)
  def toInt64OrNull(tableColumn: ConstOrColMagnet[_]): Int64    = Int64(tableColumn, orNull = true)
  def toInt64OrZero(tableColumn: ConstOrColMagnet[_]): Int64    = Int64(tableColumn, orZero = true)

  def toFloat32(tableColumn: ConstOrColMagnet[_]): Float32          = Float32(tableColumn)
  def toFloat32OrDefault(tableColumn: ConstOrColMagnet[_]): Float32 = Float32(tableColumn, orDefault = true)
  def toFloat32OrNull(tableColumn: ConstOrColMagnet[_]): Float32    = Float32(tableColumn, orNull = true)
  def toFloat32OrZero(tableColumn: ConstOrColMagnet[_]): Float32    = Float32(tableColumn, orZero = true)
  def toFloat64(tableColumn: ConstOrColMagnet[_]): Float64          = Float64(tableColumn)
  def toFloat64OrDefault(tableColumn: ConstOrColMagnet[_]): Float64 = Float64(tableColumn, orDefault = true)
  def toFloat64OrNull(tableColumn: ConstOrColMagnet[_]): Float64    = Float64(tableColumn, orNull = true)
  def toFloat64OrZero(tableColumn: ConstOrColMagnet[_]): Float64    = Float64(tableColumn, orZero = true)

  def toDate(tableColumn: ConstOrColMagnet[_]): DateRep                  = DateRep(tableColumn)
  def toDateTime(tableColumn: ConstOrColMagnet[_]): DateTimeRep          = DateTimeRep(tableColumn)
  def toDateTimeOrDefault(tableColumn: ConstOrColMagnet[_]): DateTimeRep = DateTimeRep(tableColumn)
  def toDateTimeOrNul(tableColumn: ConstOrColMagnet[_]): DateTimeRep     = DateTimeRep(tableColumn)
  def toDateTimeOrZero(tableColumn: ConstOrColMagnet[_]): DateTimeRep    = DateTimeRep(tableColumn)

  def toStringRep(tableColumn: ConstOrColMagnet[_]): StringRep             = StringRep(tableColumn)
  def toFixedString(tableColumn: ConstOrColMagnet[_], n: Int): FixedString = FixedString(tableColumn, n)
  def toStringCutToZero(tableColumn: ConstOrColMagnet[_]): StringCutToZero = StringCutToZero(tableColumn)

  def toUUID(tableColumn: ConstOrColMagnet[_]): Uuid       = Uuid(tableColumn, orZero = false, orNull = false)
  def toUUIDOrZero(tableColumn: ConstOrColMagnet[_]): Uuid = Uuid(tableColumn, orZero = true, orNull = false)
  def toUUIDOrNull(tableColumn: ConstOrColMagnet[_]): Uuid = Uuid(tableColumn, orZero = false, orNull = true)

  def reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable): Reinterpret[V] =
    Reinterpret[V](typeCastColumn)

  def cast[T <: SimpleColumnType, O](tableColumn: ConstOrColMagnet[_], simpleColumnType: T)(
      implicit castOut: CastOutBind[T, O]
  ): Cast[O] = Cast[O](tableColumn, simpleColumnType)
}
