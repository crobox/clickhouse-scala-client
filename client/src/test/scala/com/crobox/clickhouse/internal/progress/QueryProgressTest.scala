package com.crobox.clickhouse.internal.progress

import com.crobox.clickhouse.TestSpec
import com.crobox.clickhouse.internal.progress.QueryProgress.{ClickhouseQueryProgress, Progress}
import spray.json.DefaultJsonProtocol.StringJsonFormat
import spray.json._

class QueryProgressTest extends TestSpec {
  private val response: String =
    """
      |{"read_rows":"5319925","read_bytes":"215700257","written_rows":"0","written_bytes":"0","total_rows_to_read":"7004934","result_rows":"0","result_bytes":"0"}
      |""".stripMargin

  it should "parse query progress" in {
    val result = response.parseJson match {
      case JsObject(fields) =>
        ClickhouseQueryProgress(
          "queryId",
          Progress(
            fields("read_rows").convertTo[String].toLong,
            fields("read_bytes").convertTo[String].toLong,
            fields.get("written_rows").map(_.convertTo[String].toLong).getOrElse(0),
            fields.get("written_bytes").map(_.convertTo[String].toLong).getOrElse(0),
            Iterable("total_rows_to_read", "total_rows")
              .flatMap(fields.get)
              .map(_.convertTo[String].toLong)
              .headOption
              .getOrElse(0L)
          )
        )
      case unknown => throw new IllegalArgumentException(s"Cannot extract progress from $unknown")
    }
    result.progress should be(
      Progress(rowsRead = 5319925, bytesRead = 215700257, rowsWritten = 0, bytesWritten = 0, totalRows = 7004934)
    )
  }
}
