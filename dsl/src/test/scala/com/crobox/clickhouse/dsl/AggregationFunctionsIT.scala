package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.{DslLanguage, TestSchemaClickhouseQuerySpec}
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

class AggregationFunctionsIT
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures
    with DslLanguage {

  private val entries                          = 200145
  override val table1Entries: Seq[Table1Entry] = Seq.fill(entries)(Table1Entry(UUID.randomUUID()))
  override val table2Entries: Seq[Table2Entry] =
    Seq.fill(entries)(Table2Entry(UUID.randomUUID(), randomString, randomInt, randomString, None))
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  "Combinators" should "apply for aggregations" in {
    case class Result(columnResult: String) {
      def result = columnResult.toInt
    }
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "result")

    val resultSimple = chExecuter
      .execute[Result](select(simple { uniq(shieldId) } as "result") from OneTestTable)
      .futureValue
    val resultExact = chExecuter
      .execute[Result](select(exact { uniq(shieldId) } as "result") from OneTestTable)
      .futureValue
    resultSimple.rows.head.result shouldBe (entries +- entries / 100)
    resultSimple.rows.head.result should not be (entries)
    resultExact.rows.head.result shouldBe entries
  }

  it should "run quantiles" in {
    case class Result(result: Seq[Int])
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[Seq[Int], Result](Result.apply, "result")
    val result = chExecuter
      .execute[Result](
        select(simple { quantiles(col2, 0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.99F) } as "result") from TwoTestTable
      )
      .futureValue
    result.rows.head.result should have length 6
  }

}
