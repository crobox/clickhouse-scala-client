package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait TypeCastFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeTypeCastColumn(col: TypeCastColumn[_])(implicit ctx: TokenizeContext): String = {
    def tknzZero(orZero: Boolean): String = if (orZero) "OrZero" else ""
    def tknzNull(orNull: Boolean): String = if (orNull) "OrNull" else ""

    col match {
      case UInt8(tableColumn, orZero)   => s"toUInt8${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt16(tableColumn, orZero)  => s"toUInt16${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt32(tableColumn, orZero)  => s"toUInt32${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case UInt64(tableColumn, orZero)  => s"toUInt64${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int8(tableColumn, orZero)    => s"toInt8${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int16(tableColumn, orZero)   => s"toInt16${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int32(tableColumn, orZero)   => s"toInt32${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Int64(tableColumn, orZero)   => s"toInt64${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Float32(tableColumn, orZero) => s"toFloat32${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case Float64(tableColumn, orZero) => s"toFloat64${tknzZero(orZero)}(${tokenizeColumn(tableColumn.column)})"
      case DateRep(tableColumn)         => s"toDate(${tokenizeColumn(tableColumn.column)})"
      case DateTimeRep(tableColumn)     => s"toDateTime(${tokenizeColumn(tableColumn.column)})"

      case StringRep(tableColumn)       => s"toString(${tokenizeColumn(tableColumn.column)})"
      case FixedString(tableColumn, n)  => s"toFixedString(${tokenizeColumn(tableColumn.column)},$n)"
      case StringCutToZero(tableColumn) => s"toStringCutToZero(${tokenizeColumn(tableColumn.column)})"

      case UUID(tableColumn, orZero, orNull) =>
        s"toUUID${tknzZero(orZero)}${tknzNull(orNull)}(${tokenizeColumn(tableColumn.column)})"

      case Reinterpret(typeCastColumn) => s"reinterpretAs${tokenizeTypeCastColumn(typeCastColumn).substring(2)}"

      case Cast(tableColumn, simpleColumnType) => s"cast(${tokenizeColumn(tableColumn.column)} AS $simpleColumnType)"
    }
  }

}
