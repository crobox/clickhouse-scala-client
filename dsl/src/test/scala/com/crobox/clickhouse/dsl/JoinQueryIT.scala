package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.{dsl, ClickhouseClientSpec, TestSchemaClickhouseQuerySpec}
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

  it should "correct on condition for alias field" in {
    var query = select(shieldId as itemId)
      .from(OneTestTable)
      .where(notEmpty(itemId))
      .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on itemId
    var resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0

    // reverse tables to check other side of ON condition
    query = select(itemId, col2)
      .from(TwoTestTable)
      .where(notEmpty(itemId))
      .join(InnerJoin, select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))) on itemId
    resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }

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
      // TABLE -- TABLE
      var query: OperationalQuery =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(joinType, TwoTestTable) using itemId
      var resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result

      // TABLE -- QUERY
      query =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(joinType, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
      resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- TABLE
      query =
      select(dsl.all())
        .from(
          select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))
        )
        .join(joinType, TwoTestTable)
        .where(notEmpty(itemId)) using itemId
      resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- QUERY
      query =
      select(dsl.all())
        .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
        .join(joinType, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
      resultRows = chExecutor.execute[Result](query).futureValue.rows
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
    it should s"join correctly on: $joinType" in {
      assumeMinimalClickhouseVersion(20)

//      val query: OperationalQuery =
//        select(itemId).from(
//          select(itemId)
//            .from(TwoTestTable)
//            .asOfJoin(joinType, ThreeTestTable, Option("TTT"), (col2, "<="))
//            .using(itemId)
//        )

      var query: OperationalQuery =
        select(itemId)
          .from(TwoTestTable)
          .where(notEmpty(itemId))
          .join(joinType, ThreeTestTable)
          .on((col2, "<=", col2))
      var resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result

      // TABLE -- QUERY
      query = select(itemId)
        .from(TwoTestTable)
        .where(notEmpty(itemId))
        .join(joinType, select(itemId, col2).from(ThreeTestTable).where(notEmpty(itemId)))
        .on((col2, "<=", col2))
      resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- TABLE
      query = select(dsl.all())
        .from(select(itemId).from(TwoTestTable).where(notEmpty(itemId)))
        .join(joinType, ThreeTestTable)
        .where(notEmpty(itemId))
        .on((col2, "<=", col2))
      resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- QUERY
      query = select(dsl.all())
        .from(select(itemId).from(TwoTestTable).where(notEmpty(itemId)))
        .join(joinType, select(itemId, col2).from(ThreeTestTable).where(notEmpty(itemId)))
        .on((col2, "<=", col2))

      resultRows = chExecutor.execute[Result](query).futureValue.rows
      resultRows.length shouldBe result
    }
  }

  // Apparently a JOIN always require a USING column, which doesn't hold for CROSS JOIN
  it should "correctly handle cross join" in {
    val query: OperationalQuery =
      select(itemId).from(select(itemId).from(TwoTestTable).join(JoinQuery.CrossJoin, ThreeTestTable))
    println(clickhouseTokenizer.toSql(query.internalQuery))
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }

  //
  // NOW TEST FOR ALL COMBINATIONS OF JOIN
  //
  val t1 = s"captainAmerica"
  val t2 = s"twoTestTable"
  val t3 = s"threeTestTable"
  val q1 = s"$database.$t1"
  val q2 = s"$database.$t2"
  val q3 = s"$database.$t3"

  it should "TABLE using alias" in {
    val query = select(shieldId as itemId)
      .from(OneTestTable)
      .join(InnerJoin, TwoTestTable) using itemId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT shield_id AS item_id FROM $q1 INNER JOIN (SELECT * FROM $q2) AS TTT ON item_id = TTT.item_id FORMAT JSON"
    )
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }

  it should "TABLE normal" in {
    val query = select(itemId).from(TwoTestTable).join(InnerJoin, ThreeTestTable) using itemId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT item_id FROM $q2 INNER JOIN (SELECT * FROM $q3) AS TTT ON $t2.item_id = TTT.item_id FORMAT JSON"
    )
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }

  it should "OPERATIONAL using alias" in {
    val query = select(shieldId as itemId)
      .from(OneTestTable)
      .join(InnerJoin, select(itemId).from(TwoTestTable)) using itemId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT shield_id AS item_id FROM $q1 INNER JOIN (SELECT item_id FROM $q2) AS TTT ON item_id = TTT.item_id FORMAT JSON"
    )
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }

  it should "OPERATIONAL normal" in {
    val query = select(itemId)
      .from(TwoTestTable)
      .join(InnerJoin, select(itemId).from(ThreeTestTable)) using itemId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT item_id FROM $q2 INNER JOIN (SELECT item_id FROM $q3) AS TTT ON $t2.item_id = TTT.item_id FORMAT JSON"
    )
    val resultRows = chExecutor.execute[Result](query).futureValue.rows
    resultRows.length shouldBe 0
  }
}
