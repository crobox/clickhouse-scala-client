package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.dsl.execution.{CompactRowParser, ResultParsingException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class CompactRowParserTest extends AnyFlatSpec with Matchers {

  private def body(header: String, types: String, rows: String*): String =
    (Seq(header, types) ++ rows).mkString("\n") + "\n"

  it should "decode a row positionally, with no reliance on column names" in {
    val parsed = CompactRowParser.parse(
      body("""["a", "b"]""", """["String", "Int32"]""", """["x", 1]""", """["y", 2]"""),
      RowDecoder[(String, Int)]
    )
    parsed.rows shouldBe Seq(("x", 1), ("y", 2))
  }

  it should "expose the declared ClickHouse types as meta" in {
    val parsed = CompactRowParser.parse(
      body("""["n", "t"]""", """["UInt64", "DateTime"]""", """["5", "2026-08-21 10:00:00"]"""),
      RowDecoder[(Long, String)]
    )
    parsed.meta.map(_.columnTypes.map(c => c.name -> c.columnType)) shouldBe
    Some(Seq("n" -> "UInt64", "t" -> "DateTime"))
  }

  it should "decode a 64-bit integer whether ClickHouse quoted it or not" in {
    // output_format_json_quote_64bit_integers defaulted to 1 through 25.3 and to 0 from 25.8. Both shapes decode, which
    // is the reason this layer does not need that setting pinned.
    def one(value: String) =
      CompactRowParser.parse(body("""["b"]""", """["UInt64"]""", s"""[$value]"""), RowDecoder[Long]).rows

    one("\"18000\"") shouldBe Seq(18000L)
    one("18000") shouldBe Seq(18000L)
  }

  it should "decode an integer-valued column into Double, as the Sum phantom type requires" in {
    // sum(intColumn) is declared AggregateFunction[Double] but ClickHouse returns UInt64.
    CompactRowParser.parse(body("""["s"]""", """["UInt64"]""", """["123"]"""), RowDecoder[Double]).rows shouldBe
    Seq(123.0d)
  }

  it should "decode Nullable as Option" in {
    val parsed = CompactRowParser.parse(
      body("""["n"]""", """["Nullable(Int32)"]""", """[null]""", """[7]"""),
      RowDecoder[Option[Int]]
    )
    parsed.rows shouldBe Seq(None, Some(7))
  }

  it should "decode arrays and UUIDs" in {
    val id = UUID.randomUUID()
    CompactRowParser
      .parse(
        body("""["arr", "id"]""", """["Array(UInt8)", "UUID"]""", s"""[[1,2,3], "$id"]"""),
        RowDecoder[(Seq[Int], UUID)]
      )
      .rows shouldBe Seq((Seq(1, 2, 3), id))
  }

  it should "handle an empty result" in {
    CompactRowParser.parse(body("""["a"]""", """["String"]"""), RowDecoder[String]).rows shouldBe empty
  }

  it should "say which column failed, by name" in {
    val failure = intercept[RowDecodingException](
      CompactRowParser.parse(
        body("""["a", "count"]""", """["String", "UInt64"]""", """["x", "abc"]"""),
        RowDecoder[(String, Long)]
      )
    )
    failure.getMessage should include("'count'")
    failure.getMessage should include("position 2")
  }

  it should "refuse a decoder whose arity disagrees with the select list" in {
    // The failure the string-keyed JsonReader approach could not produce at all.
    val failure = intercept[ResultParsingException](
      CompactRowParser.parse(
        body("""["a", "b", "c"]""", """["String", "String", "String"]"""),
        RowDecoder[(String, String)]
      )
    )
    failure.getMessage should include("3 column(s)")
    failure.getMessage should include("expects 2")
  }

  it should "reject a truncated response rather than returning an empty result" in
    intercept[ResultParsingException](CompactRowParser.parse("""["a"]""", RowDecoder[String]))
}
