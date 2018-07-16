package com.crobox.clickhouse.dsl.language


trait RandomFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeRandomFunction(col: RandomFunction): String = {
    case Rand() => "rand()"
    case Rand64() => "rand64()"
  }

}
