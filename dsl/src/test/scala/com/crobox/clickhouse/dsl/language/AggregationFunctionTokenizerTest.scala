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

  it should "argMin and argMax" in {
    toSQL(select(argMin(col1, col2), argMax(col1, col2)), false) should matchSQL(
      "SELECT argMin(column_1, column_2), argMax(column_1, column_2)"
    )
  }

  it should "count with and without DISTINCT" in {
    toSQL(select(count(), count(col1), countDistinct(col1)), false) should matchSQL(
      "SELECT count(), count(column_1), count(DISTINCT column_1)"
    )
  }

  it should "combine argMin with a combinator" in {
    toSQL(select(aggIf(col1.isEq("abc"))(argMax(col1, col2))), false) should matchSQL(
      "SELECT argMaxIf(column_1, column_2, column_1 = 'abc')"
    )
  }

  it should "render the whole uniq family" in {
    toSQL(select(uniq(col1), uniqCombined(col1), uniqCombined64(col1), uniqExact(col1), uniqHLL12(col1)), false) should
    matchSQL(
      "SELECT uniq(column_1), uniqCombined(column_1), uniqCombined64(column_1), uniqExact(column_1), " +
        "uniqHLL12(column_1)"
    )
  }

  // HLL_precision goes in a parameter list of its own, ahead of the columns.
  it should "render HLL_precision as a separate parameter list" in {
    toSQL(select(uniqCombined(12)(col1), uniqCombined64(20)(col1, col2)), false) should matchSQL(
      "SELECT uniqCombined(12)(column_1), uniqCombined64(20)(column_1, column_2)"
    )
  }

  it should "keep HLL_precision ahead of a combinator's own arguments" in {
    toSQL(select(aggIf(col1.isEq("abc"))(uniqCombined64(12)(col2))), false) should matchSQL(
      "SELECT uniqCombined64If(12)(column_2, column_1 = 'abc')"
    )
  }

  it should "refuse an HLL_precision outside 12 to 20" in {
    an[IllegalArgumentException] should be thrownBy uniqCombined64(11)(col1)
    an[IllegalArgumentException] should be thrownBy uniqCombined(21)(col1)
  }

  it should "refuse an HLL_precision on a variant that takes none" in {
    an[IllegalArgumentException] should be thrownBy Uniq(Seq(col1), UniqModifier.Exact, Option(12))
    an[IllegalArgumentException] should be thrownBy Uniq(Seq(col1), UniqModifier.HLL12, Option(12))
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
