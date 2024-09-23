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
    }
}
