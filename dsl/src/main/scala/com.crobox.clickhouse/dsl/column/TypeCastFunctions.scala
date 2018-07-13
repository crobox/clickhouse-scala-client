package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType

trait TypeCastFunctions  { self: Magnets =>

  abstract class TypeCastColumn[V](targetColumn: ConstOrColMagnet) extends ExpressionColumn[V](targetColumn.column)

  case class Reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable)
      extends TypeCastColumn[V](typeCastColumn)

  //Tagging of compatible
  sealed trait Reinterpretable

  case class UInt8(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt16(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt32(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt64(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Int8(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int16(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int32(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int64(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Float32(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable
  case class Float64(tableColumn: ConstOrColMagnet, orZero: Boolean = false)
      extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable

  case class DateRep(tableColumn: ConstOrColMagnet)
      extends TypeCastColumn[org.joda.time.LocalDate](tableColumn)
      with Reinterpretable
  case class DateTimeRep(tableColumn: ConstOrColMagnet)
      extends TypeCastColumn[org.joda.time.DateTime](tableColumn)
      with Reinterpretable

  case class StringRep(tableColumn: ConstOrColMagnet)           extends TypeCastColumn[String](tableColumn) with Reinterpretable
  case class FixedString(tableColumn: ConstOrColMagnet, n: Int) extends TypeCastColumn[String](tableColumn)
  case class StringCutToZero(tableColumn: ConstOrColMagnet)     extends TypeCastColumn[String](tableColumn)

  case class Cast(tableColumn: ConstOrColMagnet, simpleColumnType: SimpleColumnType)
      extends TypeCastColumn(tableColumn)

  //trait TypeCastFunctionsDsl {

    def toUInt8(tableColumn: ConstOrColMagnet)  = UInt8(tableColumn)
    def toUInt16(tableColumn: ConstOrColMagnet) = UInt16(tableColumn)
    def toUInt32(tableColumn: ConstOrColMagnet) = UInt32(tableColumn)
    def toUInt64(tableColumn: ConstOrColMagnet) = UInt64(tableColumn)

    def toInt8(tableColumn: ConstOrColMagnet)  = Int8(tableColumn)
    def toInt16(tableColumn: ConstOrColMagnet) = Int16(tableColumn)
    def toInt32(tableColumn: ConstOrColMagnet) = Int32(tableColumn)
    def toInt64(tableColumn: ConstOrColMagnet) = Int64(tableColumn)

    def toFloat32(tableColumn: ConstOrColMagnet) = Float32(tableColumn)
    def toFloat64(tableColumn: ConstOrColMagnet) = Float64(tableColumn)

    def toUInt8OrZero(tableColumn: ConstOrColMagnet)  = UInt8(tableColumn, true)
    def toUInt16OrZero(tableColumn: ConstOrColMagnet) = UInt16(tableColumn, true)
    def toUInt32OrZero(tableColumn: ConstOrColMagnet) = UInt32(tableColumn, true)
    def toUInt64OrZero(tableColumn: ConstOrColMagnet) = UInt64(tableColumn, true)

    def toInt8OrZero(tableColumn: ConstOrColMagnet)  = Int8(tableColumn, true)
    def toInt16OrZero(tableColumn: ConstOrColMagnet) = Int16(tableColumn, true)
    def toInt32OrZero(tableColumn: ConstOrColMagnet) = Int32(tableColumn, true)
    def toInt64OrZero(tableColumn: ConstOrColMagnet) = Int64(tableColumn, true)

    def toFloat32OrZero(tableColumn: ConstOrColMagnet) = Float32(tableColumn, true)
    def toFloat64OrZero(tableColumn: ConstOrColMagnet) = Float64(tableColumn, true)

    def toDate(tableColumn: ConstOrColMagnet)     = DateRep(tableColumn)
    def toDateTime(tableColumn: ConstOrColMagnet) = DateTimeRep(tableColumn)

    def toStringRep(tableColumn: ConstOrColMagnet)           = StringRep(tableColumn)
    def toFixedString(tableColumn: ConstOrColMagnet, n: Int) = FixedString(tableColumn, n)
    def toStringCutToZero(tableColumn: ConstOrColMagnet)     = StringCutToZero(tableColumn)

    def reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable) = Reinterpret[V](typeCastColumn)

    def cast(tableColumn: ConstOrColMagnet, simpleColumnType: SimpleColumnType) = Cast(tableColumn, simpleColumnType)
  //}
}
