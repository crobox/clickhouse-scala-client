package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait EmptyFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeEmptyCol(col: EmptyFunction[_])(implicit ctx: TokenizeContext): String =
    col match {
      case Empty(c)    => s"empty(${tokenizeColumn(c.column)})"
      case NotEmpty(c) => s"notEmpty(${tokenizeColumn(c.column)})"
    }
}
