package com.crobox.clickhouse.internal

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * A query that fails once ClickHouse has started streaming still returns 200 with no `X-ClickHouse-Exception-Code`
 * header -- the exception is appended to the body instead. So detection has to read the body, and how precisely it does
 * that decides whether a legitimate result row can be mistaken for a failure.
 */
class ClickhouseResponseParserTest extends AnyFlatSpec with Matchers {

  private def failureDetected(content: String): Boolean =
    ClickhouseResponseParser.StreamedExceptionMarker.findFirstMatchIn(content).isDefined

  it should "detect an exception appended after the response started streaming" in {
    // Shape taken from a real mid-stream failure: rows, then the exception appended to the same 200 response.
    val body =
      """{"x":0}{"x":0}{"x":0}{"exception":"Code: 395. DB::Exception: boom: while executing 'FUNCTION throwIf'. """ +
        """(FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.3.8.10041)"}"""
    failureDetected(body) shouldBe true
  }

  it should "detect it in the raw formats too, where the exception is appended as plain text" in {
    failureDetected("0\n1\n2\nCode: 241. DB::Exception: Memory limit (total) exceeded") shouldBe true
  }

  it should "capture the ClickHouse error code" in {
    val marker = ClickhouseResponseParser.StreamedExceptionMarker
      .findFirstMatchIn("Code: 241. DB::Exception: Memory limit exceeded")
    marker.map(_.group(1)) shouldBe Some("241")
  }

  it should "not treat a result row that merely mentions DB::Exception as a failure" in {
    // The false positive the previous `content.contains("DB::Exception")` check was open to, and the reason for the
    // FIXME it carried: querying stored log lines would trip it.
    failureDetected("""{"message":"caught DB::Exception while handling request"}""") shouldBe false
    failureDetected("DB::Exception") shouldBe false
    failureDetected("""{"error_type":"DB::Exception","count":"17"}""") shouldBe false
  }

  it should "not fire on ordinary results" in {
    failureDetected("""{"x":0}{"x":1}""") shouldBe false
    failureDetected("") shouldBe false
  }
}
