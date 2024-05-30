package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class AggregationFunctionTokenizerTest extends DslTestSpec {

  it should "arrayElement in groupArray" in {
    toSQL(select(arrayElement(groupArray(col1), 1) as "p"), false) should matchSQL(
      "SELECT groupArray(column_1)[1] AS p"
    )
  }

  it should "firstValue in groupArray" in {
    toSQL(select(firstValue(groupArray(col1)) as "p"), false) should matchSQL(
      "SELECT first_value(groupArray(column_1)) AS p"
    )
  }

  it should "lastValue in groupArray" in {
    toSQL(select(lastValue(groupArray(col1)) as "p"), false) should matchSQL(
      "SELECT last_value(groupArray(column_1)) AS p"
    )
  }

  it should "anyIf in groupArray" in {
    toSQL(select(aggIf(col1.isEq("abc"))(uniq(col2))), false) should matchSQL(
      "SELECT uniqIf(column_2, column_1 = 'abc')"
    )
  }
}
