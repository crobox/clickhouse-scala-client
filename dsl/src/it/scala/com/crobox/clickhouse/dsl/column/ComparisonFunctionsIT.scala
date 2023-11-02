package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

class ComparisonFunctionsIT extends DslITSpec {
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
    r(notEquals(1,2)) shouldBe "1"
    r(_equals(2L,2)) shouldBe "1"
    r(less(1.0,200)) shouldBe "1"
    r(greater(1L,2L)) shouldBe "0"
    r(lessOrEquals(1,2)) shouldBe "1"
    r(greaterOrEquals(1,2)) shouldBe "0"
  }
}