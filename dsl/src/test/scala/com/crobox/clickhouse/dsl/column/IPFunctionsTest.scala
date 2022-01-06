package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslIntegrationSpec
import com.crobox.clickhouse.dsl._

class IPFunctionsTest extends DslIntegrationSpec {

  "Tokenization" should "succeed for IPFunctions" in {
    val num = toUInt32(1)
    r(iPv4NumToString(num)) shouldBe "0.0.0.1"
    r(iPv4StringToNum("0.0.0.1")) shouldBe "1"
    r(iPv4NumToStringClassC(num)) shouldBe "0.0.0.xxx"
    r(iPv6NumToString(toFixedString("0", 16))) shouldBe "3000::"
    r(iPv6StringToNum("3000::")) shouldBe "0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0"
  }
}