package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.DslITSpec.IntResult
import com.crobox.clickhouse.dsl._
import com.typesafe.scalalogging.LazyLogging
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import java.util.UUID

class AggregationFunctionsIT extends DslITSpec with LazyLogging {

  private val entries = 200145
  private val delta   = 2
  override val table1Entries: Seq[Table1Entry] =
    Seq.fill(entries)(Table1Entry(UUID.randomUUID(), numbers = Seq(1, 2, 3)))
  override val table2Entries: Seq[Table2Entry] = {
    (1 to entries).map(i => Table2Entry(UUID.randomUUID(), randomString, i, randomString, None))
  }

  "Combinators" should "apply for aggregations" in {
    case class Result(columnResult: String) {
      def result = columnResult.toInt
    }
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "result")
    val resultSimple = queryExecutor
      .execute[Result](select(uniq(shieldId) as "result") from OneTestTable)
      .futureValue
    val resultExact = queryExecutor
      .execute[Result](select(uniqExact(shieldId) as "result") from OneTestTable)
      .futureValue
    resultSimple.rows.head.result shouldBe (entries ~% delta)
    resultSimple.rows.head.result should not be entries
    resultExact.rows.head.result shouldBe entries
  }

  it should "run quantiles" in {
    case class Result(result: Seq[Float])
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[Seq[Float], Result](Result.apply, "result")
    val result = queryExecutor
      .execute[Result](
        select(quantiles(col2, 0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.99F) as ref[Seq[Float]]("result")) from TwoTestTable
      )
      .futureValue
    result.rows.head.result should have length 6
  }

  it should "run for each" in {
    case class Result(result: Seq[String])
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[Seq[String], Result](Result.apply, "result")
    val result = queryExecutor
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

  it should "firstValue in aggregate" in {
    val resultRows =
      queryExecutor
        .execute[IntResult](
          select(firstValue(col2) as "result").from(select(col2).from(TwoTestTable).orderBy(col2))
        )
        .futureValue
        .rows
    resultRows.length shouldBe 1
    resultRows.map(_.result).head should be(1)
  }

  it should "lastValue in aggregate" in {
    val resultRows =
      queryExecutor
        .execute[IntResult](
          select(lastValue(col2) as "result").from(select(col2).from(TwoTestTable).orderBy(col2))
        )
        .futureValue
        .rows
    resultRows.length shouldBe 1
    resultRows.map(_.result).head should be(entries)
  }
}
