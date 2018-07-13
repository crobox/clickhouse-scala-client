package com.crobox.clickhouse.dsl.language


trait RoundingFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeRoundingFunction(col: RoundingFunction): String = {""}

}
