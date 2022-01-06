package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslIntegrationSpec
import com.crobox.clickhouse.dsl._

class JsonFunctionsTest extends DslIntegrationSpec {
  it should "succeed for JsonFunctions" in {
    val someJson = """{"foo":"bar", "baz":123, "boz":3.1415, "bool":true}"""

    r(visitParamHas(someJson, "foo")) shouldBe "1"
    r(visitParamExtractUInt(someJson, "baz")) shouldBe "123"
    r(visitParamExtractInt(someJson, "baz")) shouldBe "123"
    r(visitParamExtractFloat(someJson, "boz")) shouldBe "3.1415"
    r(visitParamExtractBool(someJson, "bool")) shouldBe "1"
    r(visitParamExtractRaw(someJson, "foo")) shouldBe "\"bar\""
    r(visitParamExtractString(someJson, "foo")) shouldBe "bar"
  }
}
