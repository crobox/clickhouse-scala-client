package com.crobox.clickhouse.dsl.language
import com.crobox.clickhouse.dsl._

class HashFunctionTest extends ColumnFunctionTest{
  "Tokenization" should "succeed for HashFunctions" in {
    val someStringData = "fooBarBaz"

    //TODO these also return the byte format, can we more properly test them?
    r(halfMD5(someStringData)) shouldBe "14009637059544572277"
    r(mD5(someStringData)).nonEmpty shouldBe true
    r(sipHash64(someStringData)).nonEmpty shouldBe true
    r(sipHash128(someStringData)).nonEmpty shouldBe true
    r(cityHash64(someStringData)).nonEmpty shouldBe true
    r(intHash32(1234)).nonEmpty shouldBe true
    r(intHash64(1234)).nonEmpty shouldBe true
    r(sHA1(someStringData)).nonEmpty shouldBe true
    r(sHA224(someStringData)).nonEmpty shouldBe true
    r(sHA256(someStringData)).nonEmpty shouldBe true

    r(uRLHash("http://www.google.nl/search": String,1: Int))
  }
}
