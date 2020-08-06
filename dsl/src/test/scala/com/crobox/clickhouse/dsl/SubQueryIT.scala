package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.{dsl, ClickhouseClientSpec, TestSchemaClickhouseQuerySpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol.{jsonFormat, _}
import spray.json.RootJsonFormat

class SubQueryIT
    extends ClickhouseClientSpec
    with TableDrivenPropertyChecks
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}
  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  case class Result(result: String)
  implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "result")

  it should "select all from INNER CLAUSE" in {
    val query = select(dsl.all())
      .from(
        select(shieldId as itemId)
          .from(OneTestTable)
          .where(notEmpty(itemId))
          .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId)), Option("TTT")) using itemId
      )
    println(clickhouseTokenizer.toSql(query.internalQuery))
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }

  it should "select all from FIRST CLAUSE" in {
    // MIND / MENTION THE BRACKETS!!!
    val query = select(dsl.all())
      .from(
        select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))
      )
      .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId)), Option("TTT")) using itemId
    println(clickhouseTokenizer.toSql(query.internalQuery))
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }
}
