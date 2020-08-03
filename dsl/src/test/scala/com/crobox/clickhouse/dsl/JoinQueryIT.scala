package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.{ClickhouseClientSpec, TestSchemaClickhouseQuerySpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol.{jsonFormat, _}
import spray.json.RootJsonFormat

class JoinQueryIT
    extends ClickhouseClientSpec
    with TableDrivenPropertyChecks
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}
  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  case class Result(result: String)
  implicit val resultFormat: RootJsonFormat[Result] = jsonFormat[String, Result](Result.apply, "result")

  forAll(
    Table(
      ("joinType", "result"),
      (JoinQuery.InnerJoin, 0),
      (JoinQuery.LeftOuterJoin, 0),
      (JoinQuery.RightOuterJoin, 0),
      (JoinQuery.FullOuterJoin, 0),
      (JoinQuery.AnyInnerJoin, 0),
      (JoinQuery.AnyLeftJoin, 0),
      (JoinQuery.AnyRightJoin, 0),
      (JoinQuery.AntiLeftJoin, 0),
      (JoinQuery.AntiRightJoin, 0),
      (JoinQuery.SemiLeftJoin, 0),
      (JoinQuery.SemiRightJoin, 0),
    )
  ) { (joinType, result) =>
    it should s"join correctly on: $joinType" in {
      val query: OperationalQuery =
        select(itemId).from(select(itemId).from(TwoTestTable).join(joinType, ThreeTestTable).using(itemId))
      val resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result
    }
  }

  forAll(
    Table(
      ("joinType", "result"),
      (JoinQuery.AsOfJoin, 0),
      (JoinQuery.AsOfLeftJoin, 0),
    )
  ) { (joinType, result) =>
    ignore should s"join correctly on: $joinType" in {
      val query: OperationalQuery =
        select(itemId).from(select(itemId).from(TwoTestTable).join(joinType, ThreeTestTable).using(itemId))
      val resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result
    }
  }

  ignore should "correctly handle cross join" in {
    val query: OperationalQuery =
      select(itemId).from(select(itemId).from(TwoTestTable).join(JoinQuery.CrossJoin, ThreeTestTable))
    //      println(clickhouseTokenizer.toSql(query.internalQuery))
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }
}
