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
  override val table1Entries =
    Seq(Table1Entry(oneId), Table1Entry(randomUUID), Table1Entry(randomUUID), Table1Entry(randomUUID))
  override val table2Entries = Seq(Table2Entry(oneId, randomString, Random.nextInt(1000) + 1, randomString, None))

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
