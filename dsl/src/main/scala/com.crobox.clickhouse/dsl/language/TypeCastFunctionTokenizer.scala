package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait TypeCastFunctionTokenizer { self: ClickhouseTokenizerModule =>
  protected def tokenizeTypeCastColumn(col: TypeCastColumn[_]): String = {
    def tknz(orZero: Boolean): String =
      if (orZero) "OrZero" else ""

    col match {
      case UInt8(tableColumn, orZero)   => fast"toUInt8${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt16(tableColumn, orZero)  => fast"toUInt16${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt32(tableColumn, orZero)  => fast"toUInt32${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt64(tableColumn, orZero)  => fast"toUInt64${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int8(tableColumn, orZero)    => fast"toInt8${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int16(tableColumn, orZero)   => fast"toInt16${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int32(tableColumn, orZero)   => fast"toInt32${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int64(tableColumn, orZero)   => fast"toInt64${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Float32(tableColumn, orZero) => fast"toFloat32${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Float64(tableColumn, orZero) => fast"toFloat64${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case DateRep(tableColumn)         => fast"toDate(${tokenizeColumn(tableColumn.column)})"
      case DateTimeRep(tableColumn)     => fast"toDateTime(${tokenizeColumn(tableColumn.column)})"

      case StringRep(tableColumn)       => fast"toString(${tokenizeColumn(tableColumn.column)})"
      case FixedString(tableColumn, n)  => fast"toFixedString(${tokenizeColumn(tableColumn.column)},$n)"
      case StringCutToZero(tableColumn) => fast"toStringCutToZero(${tokenizeColumn(tableColumn.column)})"

      case Reinterpret(typeCastColumn)  => fast"reinterpretAs${tokenizeTypeCastColumn(typeCastColumn).substring(2)}"

      case Cast(tableColumn, simpleColumnType) => fast"cast(${tokenizeColumn(tableColumn.column)} AS $simpleColumnType)"
    }
  }

}
