package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType

trait TypeCastFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeTypeCastColumn(col: TypeCastColumn[_])(implicit ctx: TokenizeContext): String = {
    def tknz[T](
        column: TableColumn[T],
        valueType: SimpleColumnType,
        orZero: Boolean,
        orNull: Boolean,
        defaultValue: Option[T]
    ): String = {
      val postfix = if (orNull) "OrNull" else if (orZero) "OrZero" else ""
      val value = defaultValue match {
        case Some(value) =>
          s"to${valueType}OrDefault(${tokenizeColumn(column)}, ${tokenizeTypeCastColumn(Cast(value.toString, valueType))})"
        case _ =>
          s"to$valueType${postfix}(${tokenizeColumn(column)})"
      }
      value
    }

    col match {
      case c: UInt8               => tknz(c.tableColumn.column, ColumnType.UInt8, c.orZero, c.orNull, c.orDefault)
      case c: UInt16              => tknz(c.tableColumn.column, ColumnType.UInt16, c.orZero, c.orNull, c.orDefault)
      case c: UInt32              => tknz(c.tableColumn.column, ColumnType.UInt32, c.orZero, c.orNull, c.orDefault)
      case c: UInt64              => tknz(c.tableColumn.column, ColumnType.UInt64, c.orZero, c.orNull, c.orDefault)
      case c: Int8                => tknz(c.tableColumn.column, ColumnType.Int8, c.orZero, c.orNull, c.orDefault)
      case c: Int16               => tknz(c.tableColumn.column, ColumnType.Int16, c.orZero, c.orNull, c.orDefault)
      case c: Int32               => tknz(c.tableColumn.column, ColumnType.Int32, c.orZero, c.orNull, c.orDefault)
      case c: Int64               => tknz(c.tableColumn.column, ColumnType.Int64, c.orZero, c.orNull, c.orDefault)
      case c: Float32             => tknz(c.tableColumn.column, ColumnType.Float32, c.orZero, c.orNull, c.orDefault)
      case c: Float64             => tknz(c.tableColumn.column, ColumnType.Float64, c.orZero, c.orNull, c.orDefault)
      case c: DateRep             => tknz(c.tableColumn.column, ColumnType.Date, c.orZero, c.orNull, c.orDefault)
      case c: DateTimeRep         => tknz(c.tableColumn.column, ColumnType.DateTime, c.orZero, c.orNull, c.orDefault)
      case c: Uuid                => tknz(c.tableColumn.column, ColumnType.UUID, c.orZero, c.orNull, c.orDefault)
      case StringRep(tableColumn) => s"toString(${tokenizeColumn(tableColumn.column)})"
      case FixedString(tableColumn, n)         => s"toFixedString(${tokenizeColumn(tableColumn.column)},$n)"
      case StringCutToZero(tableColumn)        => s"toStringCutToZero(${tokenizeColumn(tableColumn.column)})"
      case Reinterpret(typeCastColumn)         => s"reinterpretAs${tokenizeTypeCastColumn(typeCastColumn).substring(2)}"
      case Cast(tableColumn, simpleColumnType) => s"cast(${tokenizeColumn(tableColumn.column)} AS $simpleColumnType)"
    }
  }
}
