package com.crobox.clickhouse.dsl.language


trait JsonFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeJsonFunction(col: JsonFunction[_]): String = {""}

}
