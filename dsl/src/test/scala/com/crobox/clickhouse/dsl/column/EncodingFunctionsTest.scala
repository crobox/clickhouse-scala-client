package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslIntegrationSpec
import com.crobox.clickhouse.dsl._

import java.util.UUID

class EncodingFunctionsTest extends DslIntegrationSpec {

  it should "succeed for EncodingFunctions" in {
    r(hex(12)) shouldBe "0C"
    r(unhex("0C")) shouldBe "\\f"

    val someUUID = UUID.randomUUID()
    r(uUIDNumToString(toFixedString("4151302937104031", 16))).nonEmpty shouldBe true
    r(uUIDStringToNum(someUUID)).nonEmpty shouldBe true
    r(bitmaskToList(2)).nonEmpty shouldBe true
    r(bitmaskToArray(2)).nonEmpty shouldBe true
  }
}
