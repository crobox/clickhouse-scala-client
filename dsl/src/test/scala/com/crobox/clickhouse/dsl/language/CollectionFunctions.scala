package com.crobox.clickhouse.dsl.language
import com.crobox.clickhouse.dsl._

class CollectionFunctions extends ColumnFunctionTest {

  "Tokenization" should "succeed for HigherOrderFunctions" in {
    val arr1 = Seq(1L,2L,3L)

    r(arrayMap[Long,Long](_ * 2L,arr1)) shouldBe "[2,4,6]"
    r(arrayFilter[Long](_ <> 2L,arr1)) shouldBe "[1,3]"
    r(arrayExists[Long](_.isEq(2L),arr1)) shouldBe "1"
    r(arrayAll[Long](_ <= 3,arr1)) shouldBe "1"
    r(arrayAll[Long](_.isEq(2L),arr1)) shouldBe "0"
    r(arrayFirst[Long](modulo(_,2L).isEq(0),arr1)) shouldBe "2"
    r(arrayFirstIndex[Long](modulo(_,2L).isEq(0),arr1)) shouldBe "2"
    r(arraySum[Long,Long](Some(_ * 2L),arr1)) shouldBe "12"
    r(arrayCount[Long](Some(_.isEq(2L)),arr1)) shouldBe "1"
    r(arrayCumSum[Long,Long](Some(_ * 2L),arr1)) shouldBe "[2,6,12]"
    r(arraySort[Long,Double](Some(_ % 3.0),arr1)) shouldBe "[3,1,2]"
    r(arrayReverseSort[Long,Int](Some(_ % 3),arr1)) shouldBe "[2,1,3]"
  }

  it should "succeed for IN functions" in {
    val someCollection = Seq(1,4,6,9)
    val someTuple = tuple(
      constOrColMagnetFromConst(1),
      constOrColMagnetFromConst(4),
      constOrColMagnetFromConst(6),
      constOrColMagnetFromConst(9)
    )
    val inNum = 4
    val notInNum = 3

    r(const(inNum).in(someCollection)) shouldBe "1"
    r(const(inNum).notIn(someTuple)) shouldBe "0"
    r(const(notInNum).globalIn(someTuple)) shouldBe "0"
    r(const(notInNum).globalNotIn(someCollection)) shouldBe "1"
  }
}
