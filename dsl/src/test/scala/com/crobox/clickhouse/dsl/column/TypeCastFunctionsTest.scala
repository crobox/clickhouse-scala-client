package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

class TypeCastFunctionsTest extends ColumnFunctionTest {
  "Tokenization" should "succeed for TypeCastFunctions" in {
    val someStringNum = const("123")
    val someDateStr = const("2018-01-01")
    val someDateTimeStr = const("2018-01-01 12:00:00")

    r(toTypeName(toUInt8(someStringNum))) shouldBe "UInt8"
    r(toTypeName(toUInt16(someStringNum))) shouldBe "UInt16"
    r(toTypeName(toUInt32(someStringNum))) shouldBe "UInt32"
    r(toTypeName(toUInt64(someStringNum))) shouldBe "UInt64"
    r(toTypeName(toInt8(someStringNum))) shouldBe "Int8"
    r(toTypeName(toInt16(someStringNum))) shouldBe "Int16"
    r(toTypeName(toInt32(someStringNum))) shouldBe "Int32"
    r(toTypeName(toInt64(someStringNum))) shouldBe "Int64"
    r(toTypeName(toFloat32(someStringNum))) shouldBe "Float32"
    r(toTypeName(toFloat64(someStringNum))) shouldBe "Float64"
    r(toTypeName(toUInt8OrZero(someStringNum))) shouldBe "UInt8"
    r(toTypeName(toUInt16OrZero(someStringNum))) shouldBe "UInt16"
    r(toTypeName(toUInt32OrZero(someStringNum))) shouldBe "UInt32"
    r(toTypeName(toUInt64OrZero(someStringNum))) shouldBe "UInt64"
    r(toTypeName(toInt8OrZero(someStringNum))) shouldBe "Int8"
    r(toTypeName(toInt16OrZero(someStringNum))) shouldBe "Int16"
    r(toTypeName(toInt32OrZero(someStringNum))) shouldBe "Int32"
    r(toTypeName(toInt64OrZero(someStringNum))) shouldBe "Int64"
    r(toTypeName(toFloat32OrZero(someStringNum))) shouldBe "Float32"
    r(toTypeName(toFloat64OrZero(someStringNum))) shouldBe "Float64"
    r(toTypeName(toDate(someDateStr))) shouldBe "Date"
    r(toTypeName(toDateTime(someDateTimeStr))) shouldBe "DateTime"
    r(toTypeName(toStringRep(someStringNum))) shouldBe "String"
    r(toTypeName(toFixedString(someStringNum,10))) shouldBe "FixedString(10)"
    r(toTypeName(toStringCutToZero(someStringNum))) shouldBe "String"
    r(reinterpret(toStringRep(65))) shouldBe "A"
    r(toTypeName(cast(someStringNum,ColumnType.Int32))) shouldBe "Int32"
  }

}
