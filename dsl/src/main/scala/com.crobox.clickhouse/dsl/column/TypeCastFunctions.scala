package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType

trait TypeCastFunctions  { self: Magnets =>

  abstract class TypeCastColumn[V](targetColumn: AnyTableColumn) extends ExpressionColumn[V](targetColumn)

  case class Reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable)
      extends TypeCastColumn[V](typeCastColumn)

  //Tagging of compatible
  sealed trait Reinterpretable

  case class UInt8(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt16(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt32(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class UInt64(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Int8(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int16(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int32(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable
  case class Int64(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Long](tableColumn)
      with Reinterpretable

  case class Float32(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable
  case class Float64(tableColumn: AnyTableColumn, orZero: Boolean = false)
      extends TypeCastColumn[Float](tableColumn)
      with Reinterpretable

  case class DateRep(tableColumn: AnyTableColumn)
      extends TypeCastColumn[org.joda.time.LocalDate](tableColumn)
      with Reinterpretable
  case class DateTimeRep(tableColumn: AnyTableColumn)
      extends TypeCastColumn[org.joda.time.DateTime](tableColumn)
      with Reinterpretable

  case class StringRep(tableColumn: AnyTableColumn)           extends TypeCastColumn[String](tableColumn) with Reinterpretable
  case class FixedString(tableColumn: AnyTableColumn, n: Int) extends TypeCastColumn[String](tableColumn)
  case class StringCutToZero(tableColumn: AnyTableColumn)     extends TypeCastColumn[String](tableColumn)

  case class Cast(tableColumn: AnyTableColumn, simpleColumnType: SimpleColumnType)
      extends TypeCastColumn(tableColumn)

  //trait TypeCastFunctionsDsl {

    def toUInt8(tableColumn: AnyTableColumn)  = UInt8(tableColumn)
    def toUInt16(tableColumn: AnyTableColumn) = UInt16(tableColumn)
    def toUInt32(tableColumn: AnyTableColumn) = UInt32(tableColumn)
    def toUInt64(tableColumn: AnyTableColumn) = UInt64(tableColumn)

    def toInt8(tableColumn: AnyTableColumn)  = Int8(tableColumn)
    def toInt16(tableColumn: AnyTableColumn) = Int16(tableColumn)
    def toInt32(tableColumn: AnyTableColumn) = Int32(tableColumn)
    def toInt64(tableColumn: AnyTableColumn) = Int64(tableColumn)

    def toFloat32(tableColumn: AnyTableColumn) = Float32(tableColumn)
    def toFloat64(tableColumn: AnyTableColumn) = Float64(tableColumn)

    def toUInt8OrZero(tableColumn: AnyTableColumn)  = UInt8(tableColumn, true)
    def toUInt16OrZero(tableColumn: AnyTableColumn) = UInt16(tableColumn, true)
    def toUInt32OrZero(tableColumn: AnyTableColumn) = UInt32(tableColumn, true)
    def toUInt64OrZero(tableColumn: AnyTableColumn) = UInt64(tableColumn, true)

    def toInt8OrZero(tableColumn: AnyTableColumn)  = Int8(tableColumn, true)
    def toInt16OrZero(tableColumn: AnyTableColumn) = Int16(tableColumn, true)
    def toInt32OrZero(tableColumn: AnyTableColumn) = Int32(tableColumn, true)
    def toInt64OrZero(tableColumn: AnyTableColumn) = Int64(tableColumn, true)

    def toFloat32OrZero(tableColumn: AnyTableColumn) = Float32(tableColumn, true)
    def toFloat64OrZero(tableColumn: AnyTableColumn) = Float64(tableColumn, true)

    def toDate(tableColumn: AnyTableColumn)     = DateRep(tableColumn)
    def toDateTime(tableColumn: AnyTableColumn) = DateTimeRep(tableColumn)

    def toStringRep(tableColumn: AnyTableColumn)           = StringRep(tableColumn)
    def toFixedString(tableColumn: AnyTableColumn, n: Int) = FixedString(tableColumn, n)
    def toStringCutToZero(tableColumn: AnyTableColumn)     = StringCutToZero(tableColumn)

    def reinterpret[V](typeCastColumn: TypeCastColumn[_] with Reinterpretable) = Reinterpret[V](typeCastColumn)

    def cast(tableColumn: AnyTableColumn, simpleColumnType: SimpleColumnType) = Cast(tableColumn, simpleColumnType)
  //}
}
