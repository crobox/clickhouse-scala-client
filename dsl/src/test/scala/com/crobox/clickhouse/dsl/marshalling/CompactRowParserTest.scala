package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.dsl.NativeColumn
import com.crobox.clickhouse.dsl.execution.{ColumnLookupException, CompactRowParser, ResultParsingException}
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

import java.util.UUID

class CompactRowParserTest extends AnyFlatSpec with Matchers {

  private def body(header: String, types: String, rows: String*): String =
    (Seq(header, types) ++ rows).mkString("\n") + "\n"

  it should "read a column by the column itself, taking the type from it" in {
    val name  = NativeColumn[String]("name")
    val count = NativeColumn[Long]("count", ColumnType.UInt64)
    val rows  = CompactRowParser
      .parse(body("""["name", "count"]""", """["String", "UInt64"]""", """["x", 1]""", """["y", 2]"""))
      .rows

    rows.map(r => (r.get(name), r.get(count))) shouldBe Seq((Some("x"), Some(1L)), (Some("y"), Some(2L)))
  }

  it should "return None for a column the result does not contain" in {
    val absent = NativeColumn[String]("absent")
    CompactRowParser.parse(body("""["a"]""", """["String"]""", """["x"]""")).rows.head.get(absent) shouldBe None
  }

  it should "name what the result did contain when a required column is missing" in {
    val absent  = NativeColumn[String]("absent")
    val row     = CompactRowParser.parse(body("""["a", "b"]""", """["String", "String"]""", """["x", "y"]""")).rows.head
    val failure = intercept[ColumnLookupException](row(absent))
    failure.getMessage should include("'absent'")
    failure.getMessage should include("[a, b]")
  }

  it should "refuse to guess when a name appears twice" in {
    // sum(x) and avg(x) are both named after x unless aliased. Returning either silently would be worse than failing.
    val column = NativeColumn[Long]("column_2")
    val row    =
      CompactRowParser.parse(body("""["column_2", "column_2"]""", """["UInt64", "UInt64"]""", """[1, 2]""")).rows.head
    val failure = intercept[ColumnLookupException](row.get(column))
    failure.getMessage should include("appears 2 times")
  }

  it should "expose the declared ClickHouse types as meta" in {
    val parsed =
      CompactRowParser.parse(
        body("""["n", "t"]""", """["UInt64", "DateTime"]""", """["5", "2026-08-21 10:00:00"]""")
      )
    parsed.meta.map(_.columnTypes.map(c => c.name -> c.columnType)) shouldBe
    Some(Seq("n" -> "UInt64", "t" -> "DateTime"))
  }

  it should "decode a 64-bit integer whether ClickHouse quoted it or not" in {
    // output_format_json_quote_64bit_integers defaulted to 1 through 25.3 and to 0 from 25.8. Both shapes decode, which
    // is the reason this layer does not need that setting pinned.
    val b                  = NativeColumn[Long]("b", ColumnType.UInt64)
    def one(value: String) =
      CompactRowParser.parse(body("""["b"]""", """["UInt64"]""", s"""[$value]""")).rows.head.get(b)

    one("\"18000\"") shouldBe Some(18000L)
    one("18000") shouldBe Some(18000L)
  }

  it should "treat a NULL as no value" in {
    val n    = NativeColumn[Int]("n", ColumnType.Nullable(ColumnType.Int32))
    val rows = CompactRowParser.parse(body("""["n"]""", """["Nullable(Int32)"]""", """[null]""", """[7]""")).rows
    rows.map(_.get(n)) shouldBe Seq(None, Some(7))
    // raw() is the way to tell an absent column apart from a NULL one.
    rows.head.raw("n") shouldBe Some(JsNull)
  }

  it should "decode arrays and UUIDs" in {
    val id  = UUID.randomUUID()
    val arr = NativeColumn[Seq[Int]]("arr", ColumnType.Array(ColumnType.UInt8))
    val uid = NativeColumn[UUID]("id", ColumnType.UUID)
    val row = CompactRowParser
      .parse(body("""["arr", "id"]""", """["Array(UInt8)", "UUID"]""", s"""[[1,2,3], "$id"]"""))
      .rows
      .head
    (row.get(arr), row.get(uid)) shouldBe ((Some(Seq(1, 2, 3)), Some(id)))
  }

  it should "decode the date types with the spelling ClickHouse actually sends" in {
    // Verified against a live server: a space rather than the ISO T, and DateTime64 carries a fractional part.
    val d    = NativeColumn[LocalDate]("d", ColumnType.Date)
    val dt   = NativeColumn[ZonedDateTime]("dt", ColumnType.DateTime)
    val dt64 = NativeColumn[ZonedDateTime]("dt64", ColumnType.DateTime)
    val row  = CompactRowParser
      .parse(
        body(
          """["d", "dt", "dt64"]""",
          """["Date", "DateTime", "DateTime64(3)"]""",
          """["2026-08-21", "2026-08-21 21:48:17", "2026-08-21 21:48:17.250"]"""
        )
      )
      .rows
      .head

    row.get(d) shouldBe Some(LocalDate.of(2026, 8, 21))
    row.get(dt) shouldBe Some(ZonedDateTime.of(2026, 8, 21, 21, 48, 17, 0, ZoneOffset.UTC))
    row.get(dt64) shouldBe Some(ZonedDateTime.of(2026, 8, 21, 21, 48, 17, 250 * 1000000, ZoneOffset.UTC))
  }

  it should "decode every DateTime64 precision ClickHouse can send" in {
    // DateTime64(n) allows n up to 9: (0) has no fractional part at all, (3)/(6)/(9) have 3, 6 and 9 digits. Taken from
    // a live server rather than assumed.
    val dt                 = NativeColumn[ZonedDateTime]("dt", ColumnType.DateTime)
    def one(value: String) =
      CompactRowParser.parse(body("""["dt"]""", """["DateTime64(9)"]""", s"""["$value"]""")).rows.head.get(dt)

    val expected = ZonedDateTime.of(2026, 8, 21, 22, 0, 33, 250 * 1000000, ZoneOffset.UTC)
    one("2026-08-21 22:00:33") shouldBe Some(expected.withNano(0))
    one("2026-08-21 22:00:33.250") shouldBe Some(expected)
    one("2026-08-21 22:00:33.250000") shouldBe Some(expected)
    one("2026-08-21 22:00:33.250000000") shouldBe Some(expected)
  }

  // Under joda this truncated to milliseconds; ZonedDateTime carries the nanoseconds through.
  it should "keep sub-millisecond precision from DateTime64" in {
    val dt  = NativeColumn[ZonedDateTime]("dt", ColumnType.DateTime)
    val row = CompactRowParser
      .parse(body("""["dt"]""", """["DateTime64(9)"]""", """["2026-08-21 22:00:33.250000001"]"""))
      .rows
      .head
    row.get(dt).map(_.getNano) shouldBe Some(250000001)
  }

  it should "hand over the raw value for a type this layer has no decoder for" in {
    val row = CompactRowParser.parse(body("""["m"]""", """["Map(String, UInt8)"]""", """[{"a":1}]""")).rows.head
    row.raw("m") shouldBe Some(JsObject("a" -> JsNumber(1)))
  }

  it should "say which column failed to decode, and what it was declared as" in {
    val count = NativeColumn[Long]("count", ColumnType.UInt64)
    val row   =
      CompactRowParser.parse(body("""["a", "count"]""", """["String", "UInt64"]""", """["x", "abc"]""")).rows.head
    val failure = intercept[ColumnLookupException](row.get(count))
    failure.getMessage should include("'count'")
    failure.getMessage should include("UInt64")
  }

  it should "expose every field, for a result whose columns are only known at runtime" in {
    val row = CompactRowParser.parse(body("""["a", "b"]""", """["String", "UInt8"]""", """["x", 1]""")).rows.head
    row.fields shouldBe Map("a" -> JsString("x"), "b" -> JsNumber(1))
    row.names shouldBe Seq("a", "b")
  }

  it should "handle an empty result" in {
    CompactRowParser.parse(body("""["a"]""", """["String"]""")).rows shouldBe empty
  }

  it should "reject a row whose width disagrees with the header" in {
    val failure = intercept[ResultParsingException](
      CompactRowParser.parse(body("""["a", "b"]""", """["String", "String"]""", """["only-one"]"""))
    )
    failure.getMessage should include("1 value(s)")
  }

  it should "reject a truncated response rather than returning an empty result" in
    intercept[ResultParsingException](CompactRowParser.parse("""["a"]"""))
}
