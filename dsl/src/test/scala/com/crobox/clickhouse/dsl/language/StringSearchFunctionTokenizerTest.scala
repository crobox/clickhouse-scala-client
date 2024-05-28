package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class StringSearchFunctionTokenizerTest extends DslTestSpec {

  it should "strMatch" in {
    toSQL(select(strMatch("abcd", ",")), false) should matchSQL("SELECT match('abcd', ',')")
  }
}
