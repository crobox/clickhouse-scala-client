package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl.JoinQuery.AnyInnerJoin
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.column.TypeCastFunctions._
import com.crobox.clickhouse.dsl.execution.{DefaultClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.Future
import scala.util.Random

class QueryIT extends ClickhouseClientSpec with TestSchemaClickhouseQuerySpec with ScalaFutures {

  import scala.concurrent.ExecutionContext.Implicits.global
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  implicit val clickhouseClient = clickClient
  private val oneId             = UUID.randomUUID()
  override val table1Entries =
    Seq(Table1Entry(oneId), Table1Entry(randomUUID), Table1Entry(randomUUID), Table1Entry(randomUUID))
  override val table2Entries = Seq(Table2Entry(oneId, randomString, Random.nextInt(1000)+1, randomString, None))

  "querying table" should "map as result" in {

    case class Result(columnResult: String, empty: Int)
    implicit val resultFormat: RootJsonFormat[Result] =
      jsonFormat[String, Int, Result](Result.apply, "column_1", "empty")
    val results: Future[QueryResult[Result]] = chExecuter.execute[Result](
      select(shieldId as itemId, col1, col1 notEmpty () as "empty") from OneTestTable join (AnyInnerJoin, TwoTestTable) using itemId
    )
    results.futureValue.rows.map(_.columnResult) should be(table2Entries.map(_.firstColumn))
    results.futureValue.rows.map(_.empty).head should be(1)
  }

  it should "perform typecasts" in {
    implicit val patienceConfig: PatienceConfig = PatienceConfig(Span(1500, Millis),Span(150, Millis))

    type TakeIntGiveIntTypes = Column => (TypeCastColumn[_$1] with Reinterpretable) forSome {type _$1 >: Long with String with Float with Serializable}

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

    val reinterpToIntResults = reinterpToIntCast.map(caster => runQry(
      select(reinterpret(caster(col1)) as "result") from TwoTestTable
    ))

    val reinterpToStringResults = reinterpToStringCast.map(caster => runQry(
      select(reinterpret(caster(col1)) as "result") from TwoTestTable
    ))

    val stringToNum = takeStringGiveIntCast.map(caster => runQry(
      select(caster(col1) as "result") from TwoTestTable
    ))

    val takeIntResults = takeIntGiveIntCast.map(caster => runQry(
      select(caster(col2) as "result") from TwoTestTable
    )) ++ takeIntGiveStringCast.map(caster => runQry(
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
