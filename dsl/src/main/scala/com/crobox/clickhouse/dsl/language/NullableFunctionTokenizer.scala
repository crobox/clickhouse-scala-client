package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait NullableFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeNullableFunction(col: NullableFunction)(implicit ctx: TokenizeContext): String =
    col match {
      case IsNull(c)        => s"isNull(${tokenizeColumn(c.column)})"
      case IsNullable(c)    => s"isNullable(${tokenizeColumn(c.column)})"
      case IsNotNull(c)     => s"isNotNull(${tokenizeColumn(c.column)})"
      case IsZeroOrNull(c)  => s"isZeroOrNull(${tokenizeColumn(c.column)})"
      case AssumeNotNull(c) => s"assumeNotNull(${tokenizeColumn(c.column)})"
      case ToNullable(c)    => s"toNullable(${tokenizeColumn(c.column)})"
      case IfNull(c, alt)   => s"ifNull(${tokenizeColumn(c.column)}, ${tokenizeColumn(alt.column)})"
      case NullIf(c, o)     => s"nullIf(${tokenizeColumn(c.column)}, ${tokenizeColumn(o.column)})"
    }
}
