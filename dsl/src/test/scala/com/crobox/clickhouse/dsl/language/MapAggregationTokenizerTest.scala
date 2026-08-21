package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

/**
 * The `*Map` aggregates and positional tuple access had no coverage at all, which is how the tuple-access operator came
 * to emit an unbalanced parenthesis. These assert the generated SQL, because that is the only place this DSL's mistakes
 * show up -- a wrong tokenizer still type-checks.
 */
class MapAggregationTokenizerTest extends DslTestSpec {

  it should "tokenize sumMap over a stored array column" in {
    // `numbers` is a NativeColumn[Seq[Int]]; accepting ArrayColMagnet must not break stored Seq columns.
    toSQL(select(sumMap(numbers, numbers) as "m"), false) should matchSQL(
      "SELECT sumMap(numbers, numbers) AS m"
    )
  }

  it should "tokenize minMap and maxMap" in {
    toSQL(select(minMap(numbers, numbers) as "m"), false) should matchSQL("SELECT minMap(numbers, numbers) AS m")
    toSQL(select(maxMap(numbers, numbers) as "m"), false) should matchSQL("SELECT maxMap(numbers, numbers) AS m")
  }

  it should "accept an array expression, not only a stored column" in {
    // The point of taking ArrayColMagnet: an inline array is typed Iterable, and because TableColumn is covariant it
    // could never satisfy a TableColumn[Seq[_]] parameter.
    toSQL(select(sumMap(arrayOf(col2), arrayOf(col2)) as "m"), false) should matchSQL(
      "SELECT sumMap([column_2], [column_2]) AS m"
    )
  }

  it should "parenthesise the operand of tuple access" in {
    // `sumMap(a, b).2` is a ClickHouse syntax error; `(sumMap(a, b)).2` is not.
    toSQL(select(mapValues(sumMap(numbers, numbers)) as "v"), false) should matchSQL(
      "SELECT (sumMap(numbers, numbers)).2 AS v"
    )
    toSQL(select(mapKeys(sumMap(numbers, numbers)) as "k"), false) should matchSQL(
      "SELECT (sumMap(numbers, numbers)).1 AS k"
    )
  }

  it should "still tokenize tuple access over a tuple literal" in {
    toSQL(select(tupleElement[String](tuple(col1, col2), 1) as "e"), false) should matchSQL(
      "SELECT ((column_1, column_2)).1 AS e"
    )
  }

  it should "project a map aggregate through a higher-order function" in {
    // The shape this was all for: one value per key, then reduced. Previously impossible -- a lambda parameter is bound
    // to a RefColumn, so tuple access inside one could not be expressed.
    val perKey = arraySum2[Int, Int](
      (total, count) => total / count,
      mapValues(sumMap(numbers, numbers)),
      mapValues(sumMap(numbers, numbers))
    )
    toSQL(select(perKey as "v"), false) should matchSQL(
      "SELECT arraySum((x,y) -> x / y, (sumMap(numbers, numbers)).2, (sumMap(numbers, numbers)).2) AS v"
    )
  }
}
