package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.{ClickhouseClientSpec, TestSchemaClickhouseQuerySpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol.{jsonFormat, _}
import spray.json.RootJsonFormat

class UUIDFunctionsIT extends ClickhouseClientSpec with TestSchemaClickhouseQuerySpec with ScalaFutures {

  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  case class Result(result: String)
  implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "result")

  it should "handle notEmpty" in {
    val resultRows =
      chExecutor.execute[Result](select(shieldId as "result").from(OneTestTable).where(notEmpty(shieldId))).futureValue.rows
    resultRows.length shouldBe 0
  }
}
