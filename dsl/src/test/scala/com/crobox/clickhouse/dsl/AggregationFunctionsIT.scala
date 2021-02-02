package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.ClickhouseClientSpec
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

class AggregationFunctionsIT
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec {

  private val entries = 200145
  private val delta = 3
  override val table1Entries: Seq[Table1Entry] =
    Seq.fill(entries)(Table1Entry(UUID.randomUUID(), numbers = Seq(1, 2, 3)))
  override val table2Entries: Seq[Table2Entry] =
    Seq.fill(entries)(Table2Entry(UUID.randomUUID(), randomString, randomInt, randomString, None))

  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  "Combinators" should "apply for aggregations" in {
    case class Result(columnResult: String) {
      def result = columnResult.toInt
    }
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "result")
    val resultSimple = chExecutor
      .execute[Result](select(uniq(shieldId) as "result") from OneTestTable)
      .futureValue
    val resultExact = chExecutor
      .execute[Result](select(uniqExact(shieldId) as "result") from OneTestTable)
      .futureValue
    resultSimple.rows.head.result shouldBe (entries ~% delta)
    resultSimple.rows.head.result should not be entries
    resultExact.rows.head.result shouldBe entries
  }

  it should "run quantiles" in {
    case class Result(result: Seq[Int])
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[Seq[Int], Result](Result.apply, "result")
    val result = chExecutor
      .execute[Result](
        select(quantiles(col2, 0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.99F) as ref[Seq[Int]]("result")) from TwoTestTable
      )
      .futureValue
    result.rows.head.result should have length 6
  }

  it should "run for each" in {
    case class Result(result: Seq[String])
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[Seq[String], Result](Result.apply, "result")
    val result = chExecutor
      .execute[Result](
        select(forEach[Int, TableColumn[Seq[Int]], Double](numbers) { column =>
          sum(column)
        } as "result") from OneTestTable
      )
      .futureValue
    val queryResult = result.rows.head.result.map(_.toInt)
    queryResult should have length 3
    queryResult should contain theSameElementsAs Seq(entries, entries * 2, entries * 3)
  }

}
