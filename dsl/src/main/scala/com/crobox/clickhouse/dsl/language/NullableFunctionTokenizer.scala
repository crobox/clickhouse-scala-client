package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.NativeColumn
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.column.NullableFunction

trait NullableFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeNullableFunction(col: NullableFunction)(implicit ctx: TokenizeContext): String =

    col match {
      case IsNull(c) => s"isNull(${tokenizeColumn(c.column)})"
      case IsNotNull(c)            => s"isNotNull(${tokenizeColumn(c.column)})"
      case IsNullable(c)           => s"isNullable(${tokenizeColumn(c.column)})"
      case IsNotDistinctFrom(c, o) => s"isNotDistinctFrom(${tokenizeColumn(c.column)}, ${tokenizeColumn(o.column)})"
      case IsZeroOrNull(c)         => s"isZeroOrNull(${tokenizeColumn(c.column)})"
      case IfNull(c, alt)          => s"ifNull(${tokenizeColumn(c.column)}, '$alt')"
      case NullIf(c, o)            => s"nullIf(${tokenizeColumn(c.column)}, ${tokenizeColumn(o.column)})"
      case AssumeNotNull(c)        => s"assumeNotNull(${tokenizeColumn(c.column)})"
      case ToNullable(c)           => s"toNullable(${tokenizeColumn(c.column)})"
    }
}