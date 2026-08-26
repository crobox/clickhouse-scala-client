package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.marshalling.QueryValue
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType

import java.time.ZonedDateTime

trait TypeCastFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeTypeCastColumn(col: TypeCastColumn[_])(implicit ctx: TokenizeContext): String = {
    def tknz(
        column: TableColumn[_],
        valueType: SimpleColumnType,
        orZero: Boolean,
        orNull: Boolean,
        defaultLiteral: Option[String]
    ): String =
      defaultLiteral match {
        case Some(literal) =>
          s"to${valueType}OrDefault(${tokenizeColumn(column)}${Tokens.Delimiter}cast($literal AS $valueType))"
        case None =>
          val postfix = if (orNull) "OrNull" else if (orZero) "OrZero" else ""
          s"to$valueType$postfix(${tokenizeColumn(column)})"
      }

    // The default is an expression of the target type, so the value's own QueryValue is what knows its literal form.
    // toString gave '1970-01-01T00:00Z' for a ZonedDateTime and the case class's own toString -- object hash included
    // -- for a UUID, and the server can parse neither.
    def lit[T](value: T)(implicit qv: QueryValue[T]): String = qv(value)

    col match {
      case c: UInt8  => tknz(c.tableColumn.column, ColumnType.UInt8, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: UInt16 =>
        // No QueryValue[Short]; widening to Int renders the same digits.
        tknz(c.tableColumn.column, ColumnType.UInt16, c.orZero, c.orNull, c.orDefault.map(v => lit(v.toInt)))
      case c: UInt32 => tknz(c.tableColumn.column, ColumnType.UInt32, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: UInt64 => tknz(c.tableColumn.column, ColumnType.UInt64, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: Int8   => tknz(c.tableColumn.column, ColumnType.Int8, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: Int16  =>
        tknz(c.tableColumn.column, ColumnType.Int16, c.orZero, c.orNull, c.orDefault.map(v => lit(v.toInt)))
      case c: Int32       => tknz(c.tableColumn.column, ColumnType.Int32, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: Int64       => tknz(c.tableColumn.column, ColumnType.Int64, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: Float32     => tknz(c.tableColumn.column, ColumnType.Float32, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: Float64     => tknz(c.tableColumn.column, ColumnType.Float64, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: DateRep     => tknz(c.tableColumn.column, ColumnType.Date, c.orZero, c.orNull, c.orDefault.map(lit(_)))
      case c: DateTimeRep => tokenizeDateTimeRep(c)
      // The default here is already a column expression rather than a value, so it renders as one.
      case c: Uuid =>
        tknz(c.tableColumn.column, ColumnType.UUID, c.orZero, c.orNull, c.orDefault.map(tokenizeColumn))
      case StringRep(tableColumn)              => s"toString(${tokenizeColumn(tableColumn.column)})"
      case FixedString(tableColumn, n)         => s"toFixedString(${tokenizeColumn(tableColumn.column)},$n)"
      case StringCutToZero(tableColumn)        => s"toStringCutToZero(${tokenizeColumn(tableColumn.column)})"
      case Reinterpret(typeCastColumn)         => s"reinterpretAs${tokenizeTypeCastColumn(typeCastColumn).substring(2)}"
      case Cast(tableColumn, simpleColumnType) => s"cast(${tokenizeColumn(tableColumn.column)} AS $simpleColumnType)"
    }
  }

  /**
   * `toDateTimeOrDefault` is the one shape that does not fit the others: the server reads its second argument as a
   * timezone, so the default has to come third. With the default second it fails with ILLEGAL_TYPE_OF_ARGUMENT.
   *
   * The literal carries no zone of its own, so it is parsed in the default's zone rather than the server's, which is
   * what keeps it the instant the caller passed.
   */
  private def tokenizeDateTimeRep(rep: DateTimeRep)(implicit ctx: TokenizeContext): String = {
    val column = tokenizeColumn(rep.tableColumn.column)
    rep.orDefault match {
      case Some(value) =>
        val zone    = value.getZone.getId
        val literal = implicitly[QueryValue[ZonedDateTime]].apply(value)
        s"toDateTimeOrDefault($column${Tokens.Delimiter}'$zone'${Tokens.Delimiter}toDateTime($literal, '$zone'))"
      case None =>
        val postfix = if (rep.orNull) "OrNull" else if (rep.orZero) "OrZero" else ""
        s"toDateTime$postfix($column)"
    }
  }
}
