package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait RandomFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeRandomFunction(col: RandomFunction): String = col match {
    case Rand()   => "rand()"
    case Rand64() => "rand64()"
  }

}
