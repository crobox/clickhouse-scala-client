package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl.execution.{DefaultClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.{ClickhouseClient, DslITSpec}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import java.util.UUID
import scala.concurrent.Future
import scala.util.Random

class QueryIT extends DslITSpec {

  implicit val clickhouseClient: ClickhouseClient = clickClient
  private val oneId                               = UUID.randomUUID()
  override val table1Entries                      =
    Seq(Table1Entry(oneId), Table1Entry(randomUUID), Table1Entry(randomUUID), Table1Entry(randomUUID))
  override val table2Entries = Seq(Table2Entry(oneId, randomString, Random.nextInt(1000) + 1, randomString, None))

  // The clause has always rendered; until QueryResult carried `totals` the extra row the server sends was discarded.
  it should "read back the row GROUP BY WITH TOTALS adds" in {
    case class Totals(itemId: String, total: Int)
    implicit val totalsFormat: RootJsonFormat[Totals] =
      jsonFormat[String, Int, Totals](Totals.apply, "item_id", "total")

    // toInt32, because count() is a UInt64 and this client pins output_format_json_quote_64bit_integers = 1, so a
    // bare count() arrives as a JSON string.
    val query  = select(itemId, toInt32(count()) as "total").from(TwoTestTable).groupBy(itemId).withTotals
    val result = queryExecutor.execute[Totals](query).futureValue

    result.totals.map(_.total) should be(Some(result.rows.map(_.total).sum))
  }

  it should "leave totals empty for a query that did not ask for them" in {
    case class Totals(itemId: String, total: Int)
    implicit val totalsFormat: RootJsonFormat[Totals] =
      jsonFormat[String, Int, Totals](Totals.apply, "item_id", "total")

    val query = select(itemId, toInt32(count()) as "total").from(TwoTestTable).groupBy(itemId)
    queryExecutor.execute[Totals](query).futureValue.totals should be(None)
  }

  // #142's shape: a scalar subquery named in WITH, then used in the projection.
  it should "compute a share against a total named in a WITH clause" in {
    case class Share(itemId: String, share: Double)
    implicit val shareFormat: RootJsonFormat[Share] =
      jsonFormat[String, Double, Share](Share.apply, "item_id", "share")

    val total = WithScalarQuery(select(toInt32(count())).from(TwoTestTable), "total")
    val query = select(itemId, (toInt32(count()) / ref[Int]("total")) as "share")
      .from(TwoTestTable)
      .groupBy(itemId)
      .withCte(total)

    val shares = queryExecutor.execute[Share](query).futureValue.rows
    shares.map(_.share).sum shouldBe 1.0 +- 0.0001
  }

  it should "select from a CTE declared in a WITH clause" in {
    case class Row(itemId: String)
    implicit val rowFormat: RootJsonFormat[Row] = jsonFormat[String, Row](Row.apply, "item_id")

    val recent = WithTable("recent", select(itemId).from(TwoTestTable))
    val rows   = queryExecutor.execute[Row](select(itemId).from(recent).withCte(recent)).futureValue.rows
    rows.map(_.itemId) should contain theSameElementsAs table2Entries.map(_.itemId.toString)
  }

  it should "map as result" in {

    case class Result(columnResult: String, empty: Int)
    implicit val resultFormat: RootJsonFormat[Result] =
      jsonFormat[String, Int, Result](Result.apply, "column_1", "empty")
    val results: Future[QueryResult[Result]] = queryExecutor.execute[Result](
      select(shieldId as itemId, col1, notEmpty(col1) as "empty") from OneTestTable join (
        InnerJoin,
        TwoTestTable
      ) using itemId
    )
    results.futureValue.rows.map(_.columnResult) should be(table2Entries.map(_.firstColumn))
    results.futureValue.rows.map(_.empty).head should be(1)
  }

  def runQry(query: OperationalQuery): Future[String] = {
    val che = queryExecutor.asInstanceOf[DefaultClickhouseQueryExecutor]
    clickhouseClient.query(che.toSql(query.internalQuery))
  }
}
