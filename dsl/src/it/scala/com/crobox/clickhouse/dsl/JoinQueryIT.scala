package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec.StringResult
import com.crobox.clickhouse.dsl.JoinQuery.{AllLeftJoin, InnerJoin}
import com.crobox.clickhouse.{dsl, DslITSpec}
import org.scalatest.prop.TableDrivenPropertyChecks

class JoinQueryIT extends DslITSpec with TableDrivenPropertyChecks {

  it should "correct on condition for alias field" in {
    var query = select(shieldId as itemId)
      .from(OneTestTable)
      .where(notEmpty(itemId))
      .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on itemId
    var resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
    resultRows.length shouldBe 0

    // reverse tables to check other side of ON condition
    query = select(itemId, col2)
      .from(TwoTestTable)
      .where(notEmpty(itemId))
      .join(InnerJoin, select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))) on itemId
    resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
    resultRows.length shouldBe 0
  }

  forAll(
    Table(
      ("joinType", "result", "min clickhouse version"),
      (JoinQuery.InnerJoin, 0, 19),
      (JoinQuery.LeftOuterJoin, 0, 19),
      (JoinQuery.RightOuterJoin, 0, 19),
      (JoinQuery.FullOuterJoin, 0, 19),
      (JoinQuery.AnyInnerJoin, 0, 19),
      (JoinQuery.AnyLeftJoin, 0, 19),
      (JoinQuery.AnyRightJoin, 0, 19),
      (JoinQuery.AntiLeftJoin, 0, 20),
      (JoinQuery.AntiRightJoin, 0, 20),
      (JoinQuery.SemiLeftJoin, 0, 20),
      (JoinQuery.SemiRightJoin, 0, 20)
    )
  ) { (joinType, result, minClickhouseVersion) =>
    it should s"join correctly on: $joinType" in {
      assumeMinimalClickhouseVersion(minClickhouseVersion)

      // TABLE -- TABLE
      var query: OperationalQuery =
        select(shieldId as itemId)
          .from(OneTestTable)
          .where(notEmpty(itemId))
          .join(joinType, TwoTestTable) using itemId
      var resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result

      // TABLE -- QUERY
      query = select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(joinType, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
      resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- TABLE
      query = select(dsl.all)
        .from(
          select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))
        )
        .join(joinType, TwoTestTable)
        .where(notEmpty(itemId)) using itemId
      resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- QUERY
      query = select(dsl.all)
        .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
        .join(joinType, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
      resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result
    }
  }

  forAll(
    Table(
      ("joinType", "result"),
      (JoinQuery.AsOfJoin, 0),
      (JoinQuery.AsOfLeftJoin, 0)
    )
  ) { (joinType, result) =>
    it should s"join correctly on: $joinType" in {
      assumeMinimalClickhouseVersion(20)

      var query: OperationalQuery =
        select(itemId, col2)
          .from(TwoTestTable)
          .where(notEmpty(itemId))
          .join(joinType, ThreeTestTable)
          .on((itemId, "=", itemId), (col2, "<=", col2))
      var resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result

      // TABLE -- QUERY
      query = select(itemId, col2)
        .from(TwoTestTable)
        .where(notEmpty(itemId))
        .join(joinType, select(itemId, col2).from(ThreeTestTable).where(notEmpty(itemId)))
        .on((itemId, "=", itemId), (col2, "<=", col2))
      resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- TABLE
      query = select(dsl.all)
        .from(select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId)))
        .join(joinType, ThreeTestTable)
        .where(notEmpty(itemId))
        .on((itemId, "=", itemId), (col2, "<=", col2))
      resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result

      // QUERY -- QUERY
      query = select(dsl.all)
        .from(select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId)))
        .join(joinType, select(itemId, col2).from(ThreeTestTable).where(notEmpty(itemId)))
        .on((itemId, "=", itemId), (col2, "<=", col2))

      resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
      resultRows.length shouldBe result
    }
  }

  // Apparently a JOIN always require a USING column, which doesn't hold for CROSS JOIN
  it should "correctly handle cross join" in {
    val query: OperationalQuery =
      select(itemId).from(select(itemId).from(TwoTestTable).join(JoinQuery.CrossJoin, ThreeTestTable))
    val resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
    resultRows.length shouldBe 0
  }

  it should s"triple complex join query" in {
    val query =
      select(itemId)
        .from(
          select(itemId)
            .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
            .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on itemId
        )
        .join(AllLeftJoin, ThreeTestTable)
        .on(itemId)
    val resultRows = queryExecutor.execute[StringResult](query).futureValue.rows
    resultRows.length shouldBe 0
  }
}
