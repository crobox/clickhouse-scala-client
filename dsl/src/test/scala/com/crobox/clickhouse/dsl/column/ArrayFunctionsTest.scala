package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class ArrayFunctionsTest extends DslTestSpec {
  val arrayNumbers           = Array(1, 2, 3, 4)
  val arrayNumbersSerialized = "[1, 2, 3, 4]"

  it should "arrayFunction: array" in {
    toSQL(select(Array())) should be("SELECT []")
    toSQL(select(Array(1, 2))) should be("SELECT [1, 2]")
  }

  it should "arrayFunction: arrayConcat" in {
    toSQL(select(arrayConcat(Array(1)))) should be("SELECT arrayConcat([1])")
    toSQL(select(arrayConcat(Array(1), Array(2), Array(3)))) should be("SELECT arrayConcat([1], [2], [3])")
  }

  it should "arrayFunction: has" in {
    var query = select(All()).from(OneTestTable).where(has(numbers, 1))
    toSQL(query) should be("WHERE has(numbers, 1)")

    query = select(All()).from(OneTestTable).where(has(arrayNumbers, 1))
    toSQL(query) should be(s"WHERE has($arrayNumbersSerialized, 1)")
  }

  it should "arrayFunction: hasAll" in {
    var query = select(All()).from(OneTestTable).where(hasAll(numbers, Array(1, 2)))
    toSQL(query) should be("WHERE hasAll(numbers, [1, 2])")

    query = select(All()).from(OneTestTable).where(hasAll(arrayNumbers, Array(1, 2)))
    toSQL(query) should be(s"WHERE hasAll($arrayNumbersSerialized, [1, 2])")
  }

  it should "arrayFunction: hasAny" in {
    var query = select(All()).from(OneTestTable).where(hasAny(numbers, Array(1, 2)))
    toSQL(query) should be("WHERE hasAny(numbers, [1, 2])")

    query = select(All()).from(OneTestTable).where(hasAny(arrayNumbers, Array(1, 2)))
    toSQL(query) should be(s"WHERE hasAny($arrayNumbersSerialized, [1, 2])")
  }

  it should "arrayFunction: resize" in {
    var query = select(arrayResize(numbers, 4, 0))
    toSQL(query) should be("SELECT arrayResize(numbers,4,0)")

    query = select(arrayResize(arrayNumbers, 4, 0))
    toSQL(query) should be(s"SELECT arrayResize($arrayNumbersSerialized,4,0)")
  }

  it should "arrayFunction: join" in {
    toSQL(select(arrayJoin(Array(shieldId, itemId)))) should be(s"SELECT arrayJoin([shield_id, item_id])")
  }

  it should "arrayFunction: join with concat" in {
    val col = arrayConcat(Array(shieldId, itemId), Array[String]())
    toSQL(select(arrayJoin(col))) should be(s"SELECT arrayJoin(arrayConcat([shield_id, item_id], []))")
  }
}
