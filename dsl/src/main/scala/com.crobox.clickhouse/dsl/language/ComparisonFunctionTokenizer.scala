package com.crobox.clickhouse.dsl.language

trait ComparisonFunctionTokenizer { this: ClickhouseTokenizerModule =>
  def tokenizeComparisonColumn(col: ComparisonColumn[_ <: Magnet[_]]): String =
    tokenizeColumn(col.left.column) + col.operator + tokenizeColumn(col.right.column)
}

