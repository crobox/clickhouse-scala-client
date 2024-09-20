package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

trait EmptyFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeEmptyCol(col: EmptyFunction[_])(implicit ctx: TokenizeContext): String =
    col match {
      case Empty(c) =>
        c.column match {
          case NativeColumn(_, ColumnType.UUID, _, None) if !ctx.version.minimalVersion(21, 8) =>
            s"${tokenizeColumn(c.column)} == 0"
          case _ => s"empty(${tokenizeColumn(c.column)})"
        }
      case NotEmpty(c) =>
        c.column match {
          case NativeColumn(_, ColumnType.UUID, _, None) if !ctx.version.minimalVersion(21, 8) =>
            s"${tokenizeColumn(c.column)} != 0"
          case _ => s"notEmpty(${tokenizeColumn(c.column)})"
        }
      case IsNull(c) =>
        c.column match {
          case NativeColumn(_, ColumnType.UUID, _, None) if !ctx.version.minimalVersion(21, 8) =>
            s"${tokenizeColumn(c.column)} != 0"
          case _ => s"isNull(${tokenizeColumn(c.column)})"
        }
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
