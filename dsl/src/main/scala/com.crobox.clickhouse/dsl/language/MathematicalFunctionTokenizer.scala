package com.crobox.clickhouse.dsl.language


trait MathematicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeMathematicalFunction(col: MathFunctionCol): String = {""}

}
