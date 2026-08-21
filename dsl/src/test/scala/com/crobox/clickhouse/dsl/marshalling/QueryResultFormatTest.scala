package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.dsl.execution.{QueryResult, ResultParsingException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
 * The untyped `FORMAT JSON` reader. Both of its shape checks used to be non-exhaustive matches, so a response the
 * server had not been expected to send surfaced as a bare MatchError; these pin that it now says what arrived.
 */
class QueryResultFormatTest extends AnyFlatSpec with Matchers {

  private def read(json: String): QueryResult[Int] = json.parseJson.convertTo[QueryResult[Int]]

  it should "read data, meta and statistics" in {
    val result = read("""
      {"meta": [{"name": "c", "type": "UInt32"}],
       "data": [1, 2],
       "rows": 2,
       "rows_before_limit_at_least": 7}
    """)
    result.rows shouldBe Seq(1, 2)
    result.meta.map(_.columnTypes.map(c => c.name -> c.columnType)) shouldBe Some(Seq("c" -> "UInt32"))
    result.statistic.map(_.rowsRead) shouldBe Some(2L)
  }

  it should "name what arrived when the data array is missing" in {
    val thrown = the[ResultParsingException] thrownBy read("""{"error": "nope"}""")
    thrown.getMessage should include("Expected a `data` array")
    thrown.getMessage should include("nope")
  }

  it should "name the offending meta entry, not the whole response" in {
    val thrown = the[ResultParsingException] thrownBy read("""
      {"meta": [{"name": "c", "type": "UInt32"}, {"nome": "typo"}], "data": []}
    """)
    thrown.getMessage should include("Expected `name` and `type` strings")
    thrown.getMessage should include("nome")
    // The point of the change: the entry that failed, not the entire payload it was found in.
    thrown.getMessage should not include "UInt32"
  }

  it should "treat a non-array meta as absent rather than failing" in {
    read("""{"meta": null, "data": [3]}""").meta shouldBe None
  }
}
