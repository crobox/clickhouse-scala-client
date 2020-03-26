package com.crobox.clickhouse.dsl.language
import java.util.UUID

import com.crobox.clickhouse.dsl._

class EncodingFunctionTest extends ColumnFunctionTest {
  "Tokenization" should "succeed for EncodingFunctions" in {
    r(hex(12)) shouldBe "0C"
    r(unhex("0C")) shouldBe "\\f"


    val someUUID = UUID.randomUUID()
    r(uUIDNumToString(toFixedString("4151302937104031": String,16: Int))).nonEmpty shouldBe true
    r(uUIDStringToNum(someUUID)).nonEmpty shouldBe true
    r(bitmaskToList(2)).nonEmpty shouldBe true
    r(bitmaskToArray(2)).nonEmpty shouldBe true
  }
}
