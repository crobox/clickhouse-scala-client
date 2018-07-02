package com.crobox.clickhouse.dsl.language


trait IPFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeIPFunction(col: IPFunction[_]): String = {""}

}
