package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

class ComparisonFunctionTest extends ColumnFunctionTest {
  "Tokenization" should "succeed for ComparisonFunctions" in {
    val someNum = const(10L)

    r(someNum <> 3) shouldBe "1"
    r(someNum > 3) shouldBe "1"
    r(someNum < 3) shouldBe "0"
    r(someNum >= 3) shouldBe "1"
    r(someNum <= 3) shouldBe "0"
    r(someNum isEq 3) shouldBe "0"
    r(someNum === 3) shouldBe "0"
    r(someNum !== 3) shouldBe "1"
    r(notEquals(1: Int,2: Int)) shouldBe "1"
    r(_equals(2L: Long,2: Int)) shouldBe "1"
    r(less(1.0: Double,200: Int)) shouldBe "1"
    r(greater(1L: Long,2L: Long)) shouldBe "0"
    r(lessOrEquals(1: Int,2: Int)) shouldBe "1"
    r(greaterOrEquals(1: Int,2: Int)) shouldBe "0"
  }

}
