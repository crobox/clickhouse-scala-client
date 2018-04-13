package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

class AggregationFunctionsIT extends ClickhouseClientSpec with TestSchemaClickhouseQuerySpec with ScalaFutures {
  private val entries                          = 200145
  override val table1Entries: Seq[Table1Entry] = Seq.fill(entries)(Table1Entry(UUID.randomUUID()))
  import scala.concurrent.ExecutionContext.Implicits.global
  case class Result(columnResult: Int)
  implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[Int, Result](Result.apply, "result")
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  "Combinators" should "apply for aggregations" in {
    chExecuter
      .execute[Result](select(exact { uniq(shieldId) } as "result") from OneTestTable)
      .map(_.rows.head.columnResult shouldBe entries)
  }

}
