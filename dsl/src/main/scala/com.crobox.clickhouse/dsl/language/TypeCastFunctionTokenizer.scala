package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait TypeCastFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeTypeCastColumn(col: TypeCastColumn[_])(implicit ctx: TokenizeContext): String = {
    def tknz(orZero: Boolean): String =
      if (orZero) "OrZero" else ""

    col match {
      case UInt8(tableColumn, orZero)   => s"toUInt8${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt16(tableColumn, orZero)  => s"toUInt16${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt32(tableColumn, orZero)  => s"toUInt32${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt64(tableColumn, orZero)  => s"toUInt64${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int8(tableColumn, orZero)    => s"toInt8${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int16(tableColumn, orZero)   => s"toInt16${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int32(tableColumn, orZero)   => s"toInt32${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int64(tableColumn, orZero)   => s"toInt64${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Float32(tableColumn, orZero) => s"toFloat32${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Float64(tableColumn, orZero) => s"toFloat64${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case DateRep(tableColumn)         => s"toDate(${tokenizeColumn(tableColumn.column)})"
      case DateTimeRep(tableColumn)     => s"toDateTime(${tokenizeColumn(tableColumn.column)})"

      case StringRep(tableColumn)       => s"toString(${tokenizeColumn(tableColumn.column)})"
      case FixedString(tableColumn, n)  => s"toFixedString(${tokenizeColumn(tableColumn.column)},$n)"
      case StringCutToZero(tableColumn) => s"toStringCutToZero(${tokenizeColumn(tableColumn.column)})"

      case UUID(tableColumn, orZero) => s"toUUID${tknz(orZero)}(${tokenizeColumn(tableColumn.column)})"

      case Reinterpret(typeCastColumn) => s"reinterpretAs${tokenizeTypeCastColumn(typeCastColumn).substring(2)}"

      case Cast(tableColumn, simpleColumnType) => s"cast(${tokenizeColumn(tableColumn.column)} AS $simpleColumnType)"
    }
  }

}
