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

  // quantile emitted a closing paren too many -- quantile(0.5)(column_2)) -- for as long as it existed, because the
  // plural quantiles was the only one any test ever rendered.
  it should "tokenize quantile" in {
    toSQL(select(quantile(col2, 0.5f)), false) should matchSQL("SELECT quantile(0.5)(column_2)")
  }

  it should "tokenize quantile with each level modifier" in {
    toSQL(select(quantileExact(col2, 0.5f)), false) should matchSQL("SELECT quantileExact(0.5)(column_2)")
    toSQL(select(quantileTDigest(col2, 0.5f)), false) should matchSQL("SELECT quantileTDigest(0.5)(column_2)")
    toSQL(select(quantileTiming(col2, 0.5f)), false) should matchSQL("SELECT quantileTiming(0.5)(column_2)")
    toSQL(select(quantileExactWeighted(col2, col2, 0.5f)), false) should matchSQL(
      "SELECT quantileExactWeighted(0.5)(column_2, column_2)"
    )
    toSQL(select(quantileTimingWeighted(col2, col2, 0.5f)), false) should matchSQL(
      "SELECT quantileTimingWeighted(0.5)(column_2, column_2)"
    )
    toSQL(select(quantileDeterministic(col2, col2, 0.5f)), false) should matchSQL(
      "SELECT quantileDeterministic(0.5)(column_2, column_2)"
    )
  }

  it should "tokenize quantiles and median unchanged" in {
    toSQL(select(quantiles(col2, 0.25f, 0.75f)), false) should matchSQL("SELECT quantiles(0.25, 0.75)(column_2)")
    toSQL(select(median(col2, 0.5f)), false) should matchSQL("SELECT median(0.5)(column_2)")
  }
}
