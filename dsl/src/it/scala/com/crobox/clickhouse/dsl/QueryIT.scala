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
  private val oneId                               = java.util.UUID.randomUUID()
  override val table1Entries =
    Seq(Table1Entry(oneId), Table1Entry(randomUUID), Table1Entry(randomUUID), Table1Entry(randomUUID))
  override val table2Entries = Seq(Table2Entry(oneId, randomString, Random.nextInt(1000) + 1, randomString, None))

  it should "map as result" in {

    case class Result(columnResult: String, empty: Int)
    implicit val resultFormat: RootJsonFormat[Result] =
      jsonFormat[String, Int, Result](Result.apply, "column_1", "empty")
    val results: Future[QueryResult[Result]] = chExecutor.execute[Result](
      select(shieldId as itemId, col1, notEmpty(col1) as "empty") from OneTestTable join (InnerJoin, TwoTestTable) using itemId
    )
    results.futureValue.rows.map(_.columnResult) should be(table2Entries.map(_.firstColumn))
    results.futureValue.rows.map(_.empty).head should be(1)
  }

  it should "perform typecasts" in {

    type TakeIntGiveIntTypes = Column => (TypeCastColumn[_$1] with Reinterpretable) forSome {
      type _$1 >: Long with String with Float with Serializable
    }

    val takeIntGiveIntCast = Set(
      toUInt8 _,
      toUInt16 _,
      toUInt32 _,
      toUInt64 _,
      toInt8 _,
      toInt16 _,
      toInt32 _,
      toInt64 _,
      toFloat32 _,
      toFloat64 _
    )

    val takeIntGiveStringCast = Set(
      toDate _,
      toDateTime _,
      toStringRep _
    )

    val reinterpToIntCast = takeIntGiveIntCast

    val reinterpToStringCast = Set(
      toStringRep _
    )

    val takeStringGiveIntCast = Set(
      toUInt8OrZero _,
      toUInt16OrZero _,
      toUInt32OrZero _,
      toUInt64OrZero _,
      toInt8OrZero _,
      toInt16OrZero _,
      toInt32OrZero _,
      toInt64OrZero _,
      toFloat32OrZero _,
      toFloat64OrZero _,
      (col: TableColumn[_]) => toFixedString(col, 10),
      toStringCutToZero _
    )
  }

  def runQry(query: OperationalQuery): Future[String] = {
    val che = chExecutor.asInstanceOf[DefaultClickhouseQueryExecutor]
    clickhouseClient.query(che.toSql(query.internalQuery))
  }
}
