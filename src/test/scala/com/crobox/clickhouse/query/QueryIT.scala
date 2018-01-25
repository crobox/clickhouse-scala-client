package com.crobox.clickhouse.query

import java.util.UUID

import com.crobox.clickhouse.query.JoinQuery.AnyInnerJoin
import com.crobox.clickhouse.query.clickhouse.QueryResult
import com.crobox.clickhouse.test.{ClickhouseClientSpec, ClickhouseSpec, TestSchema}
import org.scalatest.concurrent.ScalaFutures
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.Future

class QueryIT
    extends ClickhouseClientSpec
    with ClickhouseSpec
    with ClickhouseQuerySpec
    with TestSchema
    with ScalaFutures {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val clickhouseClient = clickClient
  private val oneId             = UUID.randomUUID()
  override val table1Entries =
    Seq(Table1Entry(oneId), Table1Entry(randomUUID), Table1Entry(randomUUID), Table1Entry(randomUUID))
  override val table2Entries = Seq(Table2Entry(oneId, randomString, r.nextInt(1000), randomString, None))

  "querying table" should "map as result" in {

    case class Result(columnResult: String)
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "column_1")
    val results: Future[QueryResult[Result]] = chExecuter.execute[Result](
      select(shieldId as itemId, col1) from OneTestTable join (AnyInnerJoin, TwoTestTable) using itemId
    )
    results.futureValue.rows.map(_.columnResult) should be(table2Entries.map(_.firstColumn))
  }

}
