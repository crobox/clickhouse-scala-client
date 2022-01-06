package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class AggregationFunctionTokenizerTest extends DslTestSpec {

  it should "arrayElement in groupArray" in {
    toSQL(select(arrayElement(groupArray(col1), 1) as "p"), false) should matchSQL(
      "SELECT groupArray(column_1)[1] AS p"
    )
  }
}
