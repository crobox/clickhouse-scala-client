package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class ArithmeticFunctionTokenizerTest extends DslTestSpec {

  it should "tokenize divide" in {
    toSQL(select(divide(1, 2)), false) should matchSQL("SELECT 1 / 2")
  }

  it should "tokenize divide functions" in {
    toSQL(select(divide(divide(1, 2), divide(3, 4))), false) should matchSQL("SELECT (1 / 2) / (3 / 4)")
  }

  it should "tokenize add" in {
    toSQL(select(plus(1, 2)), false) should matchSQL("SELECT 1 + 2")
  }

  it should "tokenize functions with precedence" in {
    toSQL(select(plus(plus(1, 2), 3)), false) should matchSQL("SELECT (1 + 2) + 3")
    toSQL(select(plus(plus(1, 2), divide(3, 4))), false) should matchSQL("SELECT (1 + 2) + (3 / 4)")
    toSQL(select(divide(multiply(minus(1, 0.9), 100.0), 0.2)), false) should matchSQL(
      "SELECT ((1 - 0.9) * 100.0) / 0.2"
    )
  }
}
