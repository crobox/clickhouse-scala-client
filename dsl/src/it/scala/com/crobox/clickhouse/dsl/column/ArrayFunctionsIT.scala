package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

class ArrayFunctionsIT extends DslITSpec {

  it should "arrayFunction: has" in {
    execute(select(has(Array(1, 2, 3, 4), 2))).futureValue.toInt should be(1)
  }

  it should "arrayFunction: hasAny" in {
    execute(select(hasAny(Array(1, 2, 3, 4), Array(2)))).futureValue.toInt should be(1)
    execute(select(hasAny(Array(1, 2, 3, 4), Array(5)))).futureValue.toInt should be(0)
    execute(select(hasAny(Array(1, 2, 3, 4), Array(1, 2)))).futureValue.toInt should be(1)
    execute(select(hasAny(Array(1, 2, 3, 4), Array(1, 5)))).futureValue.toInt should be(1)
  }

  it should "arrayFunction: hasAll" in {
    execute(select(hasAll(Array(1, 2, 3, 4), Array(1, 2)))).futureValue.toInt should be(1)
    execute(select(hasAll(Array(1, 2, 3, 4), Array(1, 5)))).futureValue.toInt should be(0)
  }

  it should "arrayFunction: resize" in {
    execute(select(arrayResize(Array(1, 2, 3, 4), 3, 0))).futureValue should be("[1,2,3]")
    execute(select(arrayResize(Array(1, 2, 3, 4), 4, 0))).futureValue should be("[1,2,3,4]")
    execute(select(arrayResize(Array(1, 2, 3, 4), 5, 0))).futureValue should be("[1,2,3,4,0]")

    execute(select(arrayResize(Array("a", "b", "c", "d"), 3, "z"))).futureValue should be("['a','b','c']")
    execute(select(arrayResize(Array("a", "b", "c", "d"), 4, "z"))).futureValue should be("['a','b','c','d']")
    execute(select(arrayResize(Array("a", "b", "c", "d"), 5, "z"))).futureValue should be("['a','b','c','d','z']")
  }

  it should "arrayFunction: difference" in {
    execute(select(arrayDifference(Array(1, 2, 3, 4)))).futureValue should be("[0,1,1,1]")
    execute(select(arrayDifference(Array(0.1, 0.2)))).futureValue should be("[0,0.1]")
    // only numeric values / array are allowed
    an[Throwable] shouldBe thrownBy(execute(select(arrayDifference(Array("a", "b", "c", "d")))).futureValue)
  }

  it should "arrayFunction: distinct" in {
    execute(select(arrayDistinct(Array(1, 2, 2, 3, 1)))).futureValue should be("[1,2,3]")
    execute(select(arrayDistinct(Array(0.1, 0.2, 0.1, 0.1, 0.3)))).futureValue should be("[0.1,0.2,0.3]")
    execute(select(arrayDistinct(Array("a", "b", "c", "d")))).futureValue should be("['a','b','c','d']")
  }

  it should "arrayFunction: intersect" in {
    execute(select(arrayIntersect(Array(1, 2), Array(1, 3), Array(2, 3)))).futureValue should be("[]")
    execute(select(arrayIntersect(Array(1, 2), Array(1, 3), Array(1, 4)))).futureValue should be("[1]")
    execute(select(arrayIntersect(Array(1, 2, 3), Array(1, 3, 4), Array(1, 3, 5)))).futureValue should be("[1,3]")
  }

  it should "arrayFunction: reduce" in {
    execute(select(arrayReduce("max", Array(1, 2, 3)))).futureValue should be("3")
    execute(select(arrayReduce("min", Array(1, 2, 3)))).futureValue should be("1")
  }

  it should "arrayFunction: reverse" in {
    execute(select(arrayReverse(Array(1, 2, 3)))).futureValue should be("[3,2,1]")
    execute(select(arrayReverse(Array("a", "b", "c", "d")))).futureValue should be("['d','c','b','a']")
  }

  it should "arrayFunction: match" in {
    execute(select(arrayMatch(Array(1, 2, 3), Array(4, 5, 6)))).futureValue should be("0")
    execute(select(arrayMatch(Array(1, 2, 3), Array(3, 4, 5)))).futureValue should be("1")
    execute(select(arrayMatch(Array(1, 2, 3), Array(1, 2, 3)))).futureValue should be("1")
  }

  it should "arrayFunction: join" in {
    execute(select(arrayJoin(Array("1", "2")))).futureValue should be("1\n2")
  }

  it should "arrayFunction: empty" in {
    execute(select(ArrayEmpty(Array("1", "2")))).futureValue should be("0")
    execute(select(arrayEmpty(Array("1", "2")))).futureValue should be("0")
  }

  it should "arrayFunction: notEmpty" in {
    execute(select(ArrayNotEmpty(Array("1", "2")))).futureValue should be("1")
    execute(select(arrayNotEmpty(Array("1", "2")))).futureValue should be("1")
  }

  it should "arrayFunction: length" in {
    execute(select(ArrayLength(Array("1", "2")))).futureValue should be("2")
    execute(select(arrayLength(Array("1", "2")))).futureValue should be("2")
  }

  it should "arrayFunction: flatten" in {
    execute(select(ArrayFlatten(Array("1", "2")))).futureValue should be("['1','2']")
    execute(select(ArrayFlatten(Array(Array("1"), Array("2"))))).futureValue should be("['1','2']")
    execute(select(ArrayFlatten(Array(Array("1", "2"), Array("3", "4"))))).futureValue should be("['1','2','3','4']")
    execute(select(ArrayFlatten(Array(Array(Array("1")), Array(Array("2"), Array("3", "4")))))).futureValue should be(
      "['1','2','3','4']"
    )

    execute(select(arrayFlatten(Array(Array(Array("1")), Array(Array("2"), Array("3", "4")))))).futureValue should be(
      "['1','2','3','4']"
    )
  }
}
