package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

class CollectionFunctionTest extends DslITSpec {

  it should "succeed for IN functions" in {
    val someCollection = Seq(1, 4, 6, 9)
    val someTuple      = tuple(1, 4, 6, 9)
    val inNum          = 4
    val notInNum       = 3

    r(const(inNum).in(someCollection)) shouldBe "1"
    r(const(inNum).notIn(someTuple)) shouldBe "0"
    r(const(notInNum).globalIn(someTuple)) shouldBe "0"
    r(const(notInNum).globalNotIn(someCollection)) shouldBe "1"
  }
}
