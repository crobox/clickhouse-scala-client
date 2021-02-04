package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._

class HigherOrderFunctionsTest extends ColumnFunctionTest {
  val arr1 = Seq(1L, 2L, 3L)

  it should "HigherOrderFunctions: arrayAll" in {
    r(arrayAll[Long](_ <= 3, arr1)) shouldBe "1"
    r(arrayAll[Long](_.isEq(2L), arr1)) shouldBe "0"
  }

  it should "HigherOrderFunctions: arrayCount" in {
    r(arrayCount[Long](Some(_.isEq(2L)), arr1)) shouldBe "1"
    r(arrayCount[Long](None, arr1)) shouldBe "3"
  }

  it should "HigherOrderFunctions: arrayCumSum" in {
    r(arrayCumSum[Long, Long](Some(_ * 2L), arr1)) shouldBe "[2,6,12]"
    r(arrayCumSum[Long, Long](None, arr1)) shouldBe "[1,3,6]"
    r(arrayCumSum2[Long, Long]((x, y) => x * y, arr1, arr1)) shouldBe "[1,5,14]"
  }

  it should "HigherOrderFunctions: arrayExists" in {
    r(arrayExists[Long](_.isEq(2L), arr1)) shouldBe "1"
    r(arrayExists[Long](_.isEq(-1L), arr1)) shouldBe "0"
  }

  it should "HigherOrderFunctions: arrayFill" in {
    r(arrayFill[Long](_.isEq(2L), arr1)) shouldBe "[1,2,2]"
  }

  it should "HigherOrderFunctions: arrayFilter" in {
    r(arrayFilter[Long](_ <> 2L, arr1)) shouldBe "[1,3]"
    r(arrayFilter[Long](_ < 0L, arr1)) shouldBe "[]"
  }

  it should "HigherOrderFunctions: arrayFirst" in {
    r(arrayFirst[Long](modulo(_, 2L).isEq(0), arr1)) shouldBe "2"
    r(arrayFirst[Long](_ < 0, arr1)) shouldBe "0"
  }

  it should "HigherOrderFunctions: arrayFirstIndex" in {
    r(arrayFirstIndex[Long](modulo(_, 2L).isEq(0), arr1)) shouldBe "2"
    r(arrayFirstIndex[Long](_ < 0, arr1)) shouldBe "0"
  }

  it should "HigherOrderFunctions: arrayMap" in {
    r(arrayMap[Long, Long](x => x * 2L, arr1)) shouldBe "[2,4,6]"
    r(arrayMap2[Long, Long]((x, y) => x * y, arr1, arr1)) shouldBe "[1,4,9]"
    r(arrayMap3[Long, Long]((x, y, z) => x * y * z, arr1, arr1, arr1)) shouldBe "[1,8,27]"
  }

  it should "HigherOrderFunctions: arrayReverseFill" in {
    r(arrayReverseFill[Long](_.isEq(2L), arr1)) shouldBe "[2,2,3]"
  }

  it should "HigherOrderFunctions: arrayReverseSort" in {
    r(arrayReverseSort[Long, Int](Some(_ % 3), arr1)) shouldBe "[2,1,3]"
    r(arrayReverseSort[Long, Int](None, arr1)) shouldBe "[3,2,1]"
  }

  it should "HigherOrderFunctions: arraySort" in {
    r(arraySort[Long, Double](Some(_ % 3.0), arr1)) shouldBe "[3,1,2]"
    r(arraySort[Long, Double](None, arr1)) shouldBe "[1,2,3]"
  }

  it should "HigherOrderFunctions: arraySum" in {
    r(arraySum[Long, Long](Some(_ * 2L), arr1)) shouldBe "12"
    r(arraySum[Long, Long](None, arr1)) shouldBe "6"
  }
}
