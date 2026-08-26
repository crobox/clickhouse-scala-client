package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

import java.time.{ZoneId, ZoneOffset, ZonedDateTime}

class TypeCaseFunctionTokenizerTest extends DslTestSpec {

  it should "succeed for UUID functions" in {
    toSQL(
      select(toUUID(const("00000000-0000-0000-0000-000000000000")))
    ) should matchSQL("SELECT toUUID('00000000-0000-0000-0000-000000000000')")
    toSQL(select(toUUIDOrZero(const("123")))) should matchSQL("SELECT toUUIDOrZero('123')")
    toSQL(select(toUUIDOrNull(const("123")))) should matchSQL("SELECT toUUIDOrNull('123')")
  }

  // The default used to render through toString, so a ZonedDateTime became '1970-01-01T00:00Z' and a UUID became the
  // case class's own toString. The server parses neither.
  "OrDefault" should "render a datetime default the server can parse" in {
    val stamp = ZonedDateTime.of(2020, 6, 1, 12, 0, 0, 0, ZoneId.of("Europe/Amsterdam"))
    toSQL(select(toDateTimeOrDefault(col3, stamp)), false) should matchSQL(
      "SELECT toDateTimeOrDefault(column_3, 'Europe/Amsterdam', toDateTime('2020-06-01 12:00:00', 'Europe/Amsterdam'))"
    )
  }

  it should "render a date default the server can parse" in {
    val stamp = ZonedDateTime.of(2020, 6, 1, 12, 0, 0, 0, ZoneOffset.UTC)
    toSQL(select(toDateOrDefault(col3, stamp)), false) should matchSQL(
      "SELECT toDateOrDefault(column_3, cast('2020-06-01 12:00:00' AS Date))"
    )
  }

  it should "render a uuid default as the column expression it is" in {
    toSQL(select(toUUIDOrDefault(col3, toUUID(const("00000000-0000-0000-0000-000000000001")))), false) should matchSQL(
      "SELECT toUUIDOrDefault(column_3, cast(toUUID('00000000-0000-0000-0000-000000000001') AS UUID))"
    )
  }

  it should "render numeric defaults" in {
    toSQL(select(toUInt8OrDefault(col3, 7.toByte)), false) should matchSQL(
      "SELECT toUInt8OrDefault(column_3, cast(7 AS UInt8))"
    )
    toSQL(select(toUInt16OrDefault(col3, 7.toShort)), false) should matchSQL(
      "SELECT toUInt16OrDefault(column_3, cast(7 AS UInt16))"
    )
    toSQL(select(toInt16OrDefault(col3, (-7).toShort)), false) should matchSQL(
      "SELECT toInt16OrDefault(column_3, cast(-7 AS Int16))"
    )
    toSQL(select(toFloat64OrDefault(col3, 1.5)), false) should matchSQL(
      "SELECT toFloat64OrDefault(column_3, cast(1.5 AS Float64))"
    )
  }

  it should "leave the OrZero and OrNull forms alone" in {
    toSQL(select(toDateTimeOrZero(col3)), false) should matchSQL("SELECT toDateTimeOrZero(column_3)")
    toSQL(select(toDateTimeOrNull(col3)), false) should matchSQL("SELECT toDateTimeOrNull(column_3)")
    toSQL(select(toDateTime(col3)), false) should matchSQL("SELECT toDateTime(column_3)")
  }
}
