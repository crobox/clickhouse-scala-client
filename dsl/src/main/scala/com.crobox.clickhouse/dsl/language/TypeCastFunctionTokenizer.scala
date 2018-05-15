package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.column.TypeCastFunctions._
import com.dongxiguo.fastring.Fastring.Implicits._

trait TypeCastFunctionTokenizer { self: ClickhouseTokenizerModule =>
  protected def tokenizeTypeCastColumn(col: TypeCastColumn[_]): String = {
    def tknz(orZero: Boolean): String =
      if (orZero) "OrZero" else ""

    col match {
      case UInt8(tableColumn, orZero) => fast"toUInt8${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case UInt16(tableColumn, orZero) => fast"toUInt16${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case UInt32(tableColumn, orZero) => fast"toUInt32${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case UInt64(tableColumn, orZero) => fast"toUInt64${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int8(tableColumn, orZero) => fast"toInt8${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int16(tableColumn, orZero) => fast"toInt16${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int32(tableColumn, orZero) => fast"toInt32${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int64(tableColumn, orZero) => fast"toInt64${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Float32(tableColumn, orZero) => fast"toFloat32${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Float64(tableColumn, orZero) => fast"toFloat64${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case DateRep(tableColumn) => fast"toDate(${tokenizeColumn(tableColumn)})"
      case DateTimeRep(tableColumn) => fast"toDateTime(${tokenizeColumn(tableColumn)})"

      case StringRep(tableColumn) => fast"toString(${tokenizeColumn(tableColumn)})"
      case FixedString(tableColumn, n) => fast"toFixedString(${tokenizeColumn(tableColumn)},$n)"
      case StringCutToZero(tableColumn) => fast"toStringCutToZero(${tokenizeColumn(tableColumn)})"

      case Reinterpret(typeCastColumn) => "reinterpretAs" + tokenizeTypeCastColumn(typeCastColumn).substring(2)

      case Cast(tableColumn, simpleColumnType) => fast"cast(${tokenizeColumn(tableColumn)} AS $simpleColumnType)"
    }
  }

}
