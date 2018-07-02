package com.crobox.clickhouse.dsl.language


trait MiscellaneousFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeMiscellaneousFunction(col: MiscellaneousFunction): String = {""}

}
