package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait UUIDFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeUUIDCol(col: UUIDFunction[_])(implicit ctx: TokenizeContext): String = {
//    col match {
//      case Empty(c)    => s"empty(${tokenizeColumn(c.column)})"
//      case NotEmpty(c) => s"notEmpty(${tokenizeColumn(c.column)})"
//    }
    ""
  }
}
