package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl.JoinQuery.AnyInnerJoin
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.execution.{DefaultClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.Future
import scala.util.Random

class QueryIT
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val clickhouseClient = clickClient
  private val oneId             = UUID.randomUUID()
  override val table1Entries =
    Seq(Table1Entry(oneId), Table1Entry(randomUUID), Table1Entry(randomUUID), Table1Entry(randomUUID))
  override val table2Entries = Seq(Table2Entry(oneId, randomString, Random.nextInt(1000)+1, randomString, None))

  case class NumResult(columnResult: Long)
  implicit val resultFormat: RootJsonFormat[NumResult] = jsonFormat[Long, NumResult](NumResult.apply, "result")

  "querying table" should "map as result" in {
    val results: Future[QueryResult[NumResult]] = chExecuter.execute[NumResult](
      select(shieldId as itemId, col1) from OneTestTable join (AnyInnerJoin, TwoTestTable) using itemId
    )
    results.futureValue.rows.map(_.columnResult) should be(table2Entries.map(_.firstColumn))
  }

  it should "perform typecasts" in {
    type TakeIntGiveIntTypes = AnyTableColumn => (TypeCastColumn[_$1] with Reinterpretable) forSome {type _$1 >: Long with String with Float}

    val takeIntGiveIntCast: Set[TakeIntGiveIntTypes] = Set(
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
      (col: AnyTableColumn) => toFixedString(col,10),
      toStringCutToZero _
    )

    val takeInt = takeIntGiveIntCast ++ takeIntGiveStringCast

    implicit val patienceConfig = PatienceConfig(Span(1500, Millis),Span(150, Millis))

    val reinterpToIntResults = reinterpToIntCast.map(caster => runQry(
      select(reinterpret(caster(col1)) as "result") from TwoTestTable
    ))

    val reinterpToStringResults = reinterpToStringCast.map(caster => runQry(
      select(reinterpret(caster(col1)) as "result") from TwoTestTable
    ))

    val stringToNum = takeStringGiveIntCast.map(caster => runQry(
      select(caster(col1) as "result") from TwoTestTable
    ))

    val takeIntResults = takeInt.map(caster => runQry(
      select(caster(col2) as "result") from TwoTestTable
    ))

    val outcomes = takeIntResults ++ reinterpToStringResults ++
      reinterpToIntResults ++ stringToNum

    Future
      .sequence(outcomes)
      .futureValue
      .foreach(_.length should be > 1)
  }

  def runQry(query: OperationalQuery): Future[String] = {
    val che = chExecuter.asInstanceOf[DefaultClickhouseQueryExecutor]
    clickhouseClient.query(che.toSql(query.internalQuery)(clickhouseClient.database))
  }
}
