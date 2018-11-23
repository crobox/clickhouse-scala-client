package com.crobox.clickhouse.dsl.language

import java.util.UUID

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.ClickhouseClientSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol.{jsonFormat, _}
import spray.json.RootJsonFormat

class StringFunctionsIT
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures {

  private val columnString = "oneem,twoem,threeem"
  override val table2Entries: Seq[Table2Entry] =
    Seq(Table2Entry(UUID.randomUUID(), columnString, randomInt, randomString, None))

  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  case class Result(result: String)
  implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "result")

  it should "split by character" in {
    val resultRows =
      chExecuter.execute[Result](select(arrayJoin(splitByChar(",", col1)) as "result") from TwoTestTable).futureValue.rows
    resultRows.length shouldBe 3
    resultRows.map(_.result) should contain theSameElementsAs Seq("oneem", "twoem", "threeem")
  }

  it should "split by string" in {
    val resultRows =
      chExecuter.execute[Result](select(arrayJoin(splitByString("em,", col1)) as "result") from TwoTestTable).futureValue.rows
    resultRows.length shouldBe 3
    resultRows.map(_.result) should contain theSameElementsAs Seq("one", "two", "threeem")
  }

  it should "concatenate string back" in {
    val resultRows =
      chExecuter
        .execute[Result](select(arrayStringConcat(splitByChar(",", col1), ",") as "result") from TwoTestTable)
        .futureValue
        .rows
    resultRows.length shouldBe 1
    resultRows.map(_.result).head shouldBe columnString

  }

}
