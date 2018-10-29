package com.crobox.clickhouse.dsl.language
import com.crobox.clickhouse.dsl._

class JsonFunctionTest extends ColumnFunctionTest {
  "JSON Function tokenisation" should "succeed for JsonFunctions" in {
    val someJson = """{"foo":"bar", "baz":123, "boz":3.1415, "bool":true}"""

    r(visitParamHas(someJson,"foo")) shouldBe "1"
    r(visitParamExtractUInt(someJson,"baz")) shouldBe "123"
    r(visitParamExtractInt(someJson,"baz")) shouldBe "123"
    r(visitParamExtractFloat(someJson,"boz")) shouldBe "3.1415"
    r(visitParamExtractBool(someJson,"bool")) shouldBe "1"
    r(visitParamExtractRaw(someJson,"foo")) shouldBe "\"bar\""
    r(visitParamExtractString(someJson,"foo")) shouldBe "bar"
  }
}
