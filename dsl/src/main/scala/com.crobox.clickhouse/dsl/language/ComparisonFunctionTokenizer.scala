package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database

trait ComparisonFunctionTokenizer { this: ClickhouseTokenizerModule =>
  def tokenizeComparisonColumn(col: ComparisonColumn)(implicit database: Database): String =
    tokenizeColumn(col.left.column) + " " + col.operator + " " + tokenizeColumn(col.right.column)
}

