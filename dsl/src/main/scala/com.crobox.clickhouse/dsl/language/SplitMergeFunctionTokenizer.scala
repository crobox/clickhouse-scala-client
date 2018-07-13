package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._


trait SplitMergeFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeSplitMergeFunctionn(col: SplitMergeFunction[_]): String = {""}

}
