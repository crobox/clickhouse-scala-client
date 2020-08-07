package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.{dsl, ClickhouseClientSpec}
import org.scalatest.prop.TableDrivenPropertyChecks

class JoinQueryTest extends ClickhouseClientSpec with TableDrivenPropertyChecks with TestSchema {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}
  val database            = "sc"

  it should s"join correctly on: ${JoinQuery.CrossJoin}" in {
    val query: OperationalQuery =
      select(itemId).from(select(itemId).from(TwoTestTable).join(JoinQuery.CrossJoin, ThreeTestTable))
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT item_id FROM (SELECT item_id FROM sc.twoTestTable AS l1 CROSS JOIN (SELECT * FROM sc.threeTestTable) AS r1) FORMAT JSON"
    )
  }

  it should s"TABLE - TABLE - using" in {
    val query: OperationalQuery =
    select(shieldId as itemId)
      .from(OneTestTable)
      .where(notEmpty(itemId))
      .join(InnerJoin, TwoTestTable) using itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT shield_id AS item_id FROM sc.captainAmerica AS l1 INNER JOIN (SELECT * FROM sc.twoTestTable) AS r1 USING item_id WHERE notEmpty(item_id) FORMAT JSON"
    )
  }

  it should s"TABLE - QUERY - using" in {
    val query =
    select(shieldId as itemId)
      .from(OneTestTable)
      .where(notEmpty(itemId))
      .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT shield_id AS item_id FROM sc.captainAmerica AS l1 INNER JOIN (SELECT item_id, column_2 FROM sc.twoTestTable WHERE notEmpty(item_id)) AS r1 USING item_id WHERE notEmpty(item_id) FORMAT JSON"
    )
  }

  it should s"QUERY - TABLE - using" in {
    val query =
    select(dsl.all())
      .from(
        select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))
      )
      .join(InnerJoin, TwoTestTable)
      .where(notEmpty(itemId)) using itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM sc.captainAmerica WHERE notEmpty(item_id)) AS l1 INNER JOIN (SELECT * FROM sc.twoTestTable) AS r1 USING item_id WHERE notEmpty(item_id) FORMAT JSON"
    )
  }

  it should s"QUERY - QUERY - using" in {
    val query =
    select(dsl.all())
      .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
      .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM sc.captainAmerica WHERE notEmpty(item_id)) AS l1 INNER JOIN (SELECT item_id, column_2 FROM sc.twoTestTable WHERE notEmpty(item_id)) AS r1 USING item_id FORMAT JSON"
    )
  }

  // ON --> check prefix per ON condition
  it should s"QUERY - QUERY - on simple" in {
    val query =
    select(dsl.all())
      .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
      .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM sc.captainAmerica WHERE notEmpty(item_id)) AS l1 INNER JOIN (SELECT item_id, column_2 FROM sc.twoTestTable WHERE notEmpty(item_id)) AS r1 ON l1.item_id = r1.item_id FORMAT JSON"
    )
  }

  // ON --> check prefix per ON condition
  it should s"QUERY - QUERY - on complex" in {
    val query =
    select(dsl.all())
      .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
      .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on ((itemId, "<=", itemId))
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM sc.captainAmerica WHERE notEmpty(item_id)) AS l1 INNER JOIN (SELECT item_id, column_2 FROM sc.twoTestTable WHERE notEmpty(item_id)) AS r1 ON l1.item_id <= r1.item_id FORMAT JSON"
    )
  }

  it should s"fail on empty on and using" in {
    val query: OperationalQuery =
    select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)).join(InnerJoin, TwoTestTable)
    an[AssertionError] shouldBe thrownBy(clickhouseTokenizer.toSql(query.internalQuery))
  }

  it should s"fail on set on and using" in {
    val query: OperationalQuery =
      select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)).join(InnerJoin, TwoTestTable) using itemId on itemId
    an[AssertionError] shouldBe thrownBy(clickhouseTokenizer.toSql(query.internalQuery))
  }
}
