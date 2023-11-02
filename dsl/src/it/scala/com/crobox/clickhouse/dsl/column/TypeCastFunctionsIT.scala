package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

class TypeCastFunctionsIT extends DslITSpec {

  it should "handle UInt8" in {
    r(toUInt8("1")) should be("1")
    r(toUInt8(1)) should be("1")

    r(toUInt8(Byte.MaxValue.toString)) should be("127")
    r(toUInt8(Byte.MaxValue)) should be("127")

    r(toUInt8(Short.MaxValue.toString)) should be("255")

    r(toUInt8(Int.MaxValue.toString)) should be("255")
    r(toUInt8(Int.MaxValue)) should be("255")

    r(toUInt8(Long.MaxValue.toString)) should be("255")
    r(toUInt8(Long.MaxValue)) should be("255")

    an[Exception] should be thrownBy r(toUInt8(Float.MaxValue.toString))
    r(toUInt8(Float.MaxValue)) should be("0")

    an[Exception] should be thrownBy r(toUInt8(Double.MaxValue.toString))
    r(toUInt8(Double.MaxValue)) should be("0")
  }

  it should "handle UInt32" in {
    // Unsigned can't handle NEGATIVE numbers
    r(toUInt32(Byte.MaxValue.toString)) should be("127")
    r(toUInt32(Byte.MaxValue)) should be("127")
    an[Exception] should be thrownBy r(toUInt32(Byte.MinValue.toString))
    r(toUInt32(Byte.MinValue)) should be("4294967168")

    r(toUInt32(Short.MaxValue.toString)) should be("32767")
    an[Exception] should be thrownBy r(toUInt32(Short.MinValue.toString))

    r(toUInt32(Int.MaxValue.toString)) should be("2147483647")
    r(toUInt32(Int.MaxValue)) should be("2147483647")
    an[Exception] should be thrownBy r(toUInt32(Int.MinValue.toString))
    r(toUInt32(Int.MinValue)) should be("2147483648")

    r(toUInt32(Long.MaxValue.toString)) should be("4294967295")
    r(toUInt32(Long.MaxValue)) should be("4294967295")
    an[Exception] should be thrownBy r(toUInt32(Long.MinValue.toString))
    r(toUInt32(Long.MinValue)) should be("0")

    an[Exception] should be thrownBy r(toUInt32(Float.MaxValue.toString))
    r(toUInt32(Float.MaxValue)) should be("0")
    an[Exception] should be thrownBy r(toUInt32(Float.MinValue.toString))
    r(toUInt32(Float.MinValue)) should be("0")

    an[Exception] should be thrownBy r(toUInt32(Double.MaxValue.toString))
    r(toUInt32(Double.MaxValue)) should be("0")
    an[Exception] should be thrownBy r(toUInt32(Double.MinValue.toString))
    r(toUInt32(Double.MinValue)) should be("0")

    an[Exception] should be thrownBy r(toUInt32(""))
    an[Exception] should be thrownBy r(toUInt32("error"))
    an[Exception] should be thrownBy r(toUInt32("er\nror"))
    an[Exception] should be thrownBy r(toUInt32("12\n58"))
    an[Exception] should be thrownBy r(toUInt32("12\n58 gram"))
  }

  // this is the java/scala int (since we need 1 byte for negative)
  it should "handle Int32" in {
    r(toInt32(Byte.MaxValue.toString)) should be("127")
    r(toInt32(Byte.MaxValue)) should be("127")
    r(toInt32(Byte.MinValue.toString)) should be("-128")
    r(toInt32(Byte.MinValue)) should be("-128")

    r(toInt32(Short.MaxValue.toString)) should be("32767")
    r(toInt32(Short.MinValue.toString)) should be("-32768")

    r(toInt32(Int.MaxValue.toString)) should be("2147483647")
    r(toInt32(Int.MaxValue)) should be("2147483647")
    r(toInt32(Int.MinValue.toString)) should be("-2147483648")
    r(toInt32(Int.MinValue)) should be("-2147483648")

    r(toInt32(Long.MaxValue.toString)) should be("-1")
    r(toInt32(Long.MaxValue)) should be("-1")
    r(toInt32(Long.MinValue.toString)) should be("0")
    r(toInt32(Long.MinValue)) should be("0")

    an[Exception] should be thrownBy r(toInt32(Float.MaxValue.toString))
    r(toInt32(Float.MaxValue)) should be("-2147483648")
    an[Exception] should be thrownBy r(toInt32(Float.MinValue.toString))
    r(toInt32(Float.MinValue)) should be("-2147483648")

    an[Exception] should be thrownBy r(toInt32(Double.MaxValue.toString))
    r(toInt32(Double.MaxValue)) should be("-2147483648")
    an[Exception] should be thrownBy r(toInt32(Double.MinValue.toString))
    r(toInt32(Double.MinValue)) should be("-2147483648")

    an[Exception] should be thrownBy r(toInt32(""))
    an[Exception] should be thrownBy r(toInt32("error"))
    an[Exception] should be thrownBy r(toInt32("er\nror"))
    an[Exception] should be thrownBy r(toInt32("12\n58"))
    an[Exception] should be thrownBy r(toInt32("12\n58 gram"))
  }

  it should "handle Int32OrZero" in {
    // orZero only accept STRING
    an[Exception] should be thrownBy r(toInt32OrZero(Int.MaxValue))

    r(toInt32OrZero("1")) should be("1")
    r(toInt32OrZero(Byte.MaxValue.toString)) should be("127")
    r(toInt32OrZero(Byte.MinValue.toString)) should be("-128")
    r(toInt32OrZero(Short.MaxValue.toString)) should be("32767")
    r(toInt32OrZero(Short.MinValue.toString)) should be("-32768")
    r(toInt32OrZero(Int.MaxValue.toString)) should be("2147483647")
    r(toInt32OrZero(Int.MinValue.toString)) should be("-2147483648")
    r(toInt32OrZero(Long.MaxValue.toString)) should be("0")
    r(toInt32OrZero(Long.MinValue.toString)) should be("0")
    r(toInt32OrZero(Float.MaxValue.toString)) should be("0")
    r(toInt32OrZero(Float.MinValue.toString)) should be("0")
    r(toInt32OrZero(Double.MaxValue.toString)) should be("0")
    r(toInt32OrZero(Double.MinValue.toString)) should be("0")

    r(toInt32OrZero("")) should be("0")
    r(toInt32OrZero("error")) should be("0")
    r(toInt32OrZero("er\nror")) should be("0")
    r(toInt32OrZero("12\n58")) should be("0")
    r(toInt32OrZero("12\n58 gram")) should be("0")
  }

  it should "handle Int32OrNull" in {
    // orNull only accept STRING
    an[Exception] should be thrownBy r(toInt32OrZero(Int.MaxValue))

    r(toInt32OrNull("1")) should be("1")
    r(toInt32OrNull(Byte.MaxValue.toString)) should be("127")
    r(toInt32OrNull(Byte.MinValue.toString)) should be("-128")
    r(toInt32OrNull(Short.MaxValue.toString)) should be("32767")
    r(toInt32OrNull(Short.MinValue.toString)) should be("-32768")
    r(toInt32OrNull(Int.MaxValue.toString)) should be("2147483647")
    r(toInt32OrNull(Int.MinValue.toString)) should be("-2147483648")
    r(toInt32OrNull(Long.MaxValue.toString)) should be("\\N")
    r(toInt32OrNull(Long.MinValue.toString)) should be("\\N")
    r(toInt32OrNull(Float.MaxValue.toString)) should be("\\N")
    r(toInt32OrNull(Float.MinValue.toString)) should be("\\N")
    r(toInt32OrNull(Double.MaxValue.toString)) should be("\\N")
    r(toInt32OrNull(Double.MinValue.toString)) should be("\\N")

    r(toInt32OrNull("")) should be("\\N")
    r(toInt32OrNull("error")) should be("\\N")
    r(toInt32OrNull("er\nror")) should be("\\N")
    r(toInt32OrNull("12\n58")) should be("\\N")
    r(toInt32OrNull("12\n58 gram")) should be("\\N")
  }

  it should "handle Int32OrDefault" in {
    // orNull only accept STRING (and only available in version 22.3 and upwards)
    assumeMinimalClickhouseVersion(22, 3)

    r(toInt32OrDefault("1", 123)) should be("1")
    r(toInt32OrDefault(Byte.MaxValue.toString, 123)) should be("127")
    r(toInt32OrDefault(Byte.MinValue.toString, 123)) should be("-128")
    r(toInt32OrDefault(Short.MaxValue.toString, 123)) should be("32767")
    r(toInt32OrDefault(Short.MinValue.toString, 123)) should be("-32768")
    r(toInt32OrDefault(Int.MaxValue.toString, 123)) should be("2147483647")
    r(toInt32OrDefault(Int.MinValue.toString, 123)) should be("-2147483648")
    r(toInt32OrDefault(Long.MaxValue.toString, 123)) should be("123")
    r(toInt32OrDefault(Long.MinValue.toString, 123)) should be("123")
    r(toInt32OrDefault(Float.MaxValue.toString, 123)) should be("123")
    r(toInt32OrDefault(Float.MinValue.toString, 123)) should be("123")
    r(toInt32OrDefault(Double.MaxValue.toString, 123)) should be("123")
    r(toInt32OrDefault(Double.MinValue.toString, 123)) should be("123")

    r(toInt32OrDefault("", 123)) should be("123")
    r(toInt32OrDefault("error", 123)) should be("123")
    r(toInt32OrDefault("er\nror", 123)) should be("123")
    r(toInt32OrDefault("12\n58", 123)) should be("123")
    r(toInt32OrDefault("12\n58 gram", 123)) should be("123")
  }

  it should "succeed for TypeCastFunctions" in {
    val someStringNum   = const("123")
    val someDateStr     = const("2018-01-01")
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
    r(toTypeName(toFixedString(someStringNum, 10))) shouldBe "FixedString(10)"
    r(toTypeName(toStringCutToZero(someStringNum))) shouldBe "String"
    r(reinterpret(toStringRep(65))) shouldBe "A"
    r(toTypeName(cast(someStringNum, ColumnType.Int32))) shouldBe "Int32"

    r(toTypeName(toUUID(const("00000000-0000-0000-0000-000000000000")))) shouldBe "UUID"
    r(toTypeName(toUUIDOrZero(const("123")))) shouldBe "UUID"
    r(toTypeName(toUUIDOrNull(const("123")))) shouldBe "Nullable(UUID)"
  }
}
