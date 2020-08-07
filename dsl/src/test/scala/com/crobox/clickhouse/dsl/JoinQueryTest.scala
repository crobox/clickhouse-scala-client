package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.{ClickhouseClientSpec, dsl}
import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import org.scalatest.prop.TableDrivenPropertyChecks

class JoinQueryTest extends ClickhouseClientSpec with TableDrivenPropertyChecks with TestSchema {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}
  val database            = "join_query"

  it should s"join correctly on: ${JoinQuery.CrossJoin}" in {
    val query: OperationalQuery =
      select(itemId).from(select(itemId).from(TwoTestTable).join(JoinQuery.CrossJoin, ThreeTestTable))
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT item_id FROM (SELECT item_id FROM join_query.twoTestTable as l1 CROSS JOIN (SELECT * FROM join_query.threeTestTable) AS r1) FORMAT JSON"
    )
  }

  it should s"TABLE - TABLE - using" in {
    val query: OperationalQuery =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT item_id FROM (SELECT item_id FROM join_query.twoTestTable as l1 CROSS JOIN (SELECT * FROM join_query.threeTestTable) AS r1) FORMAT JSON"
    )
  }

  it should s"TABLE - QUERY - using" in {
    val right = select(itemId, col2).from(
      select(dsl.all()).from(select(shieldId as itemId).from(TwoTestTable).where(notEmpty(itemId)))
    )
    val query =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(InnerJoin, right) on itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT item_id FROM (SELECT item_id FROM join_query.twoTestTable as l1 CROSS JOIN (SELECT * FROM join_query.threeTestTable) AS r1) FORMAT JSON"
    )
  }

  it should s"QUERY - TABLE - using" in {
    val query =
      select(dsl.all())
        .from(
          select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))
        )
        .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT item_id FROM (SELECT item_id FROM join_query.twoTestTable as l1 CROSS JOIN (SELECT * FROM join_query.threeTestTable) AS r1) FORMAT JSON"
    )
  }

  it should s"QUERY - QUERY - using" in {
    val right = select(itemId, col2).from(
      select(dsl.all()).from(select(shieldId as itemId).from(TwoTestTable).where(notEmpty(itemId)))
    )
    val query =
      select(dsl.all())
        .from(
          select(shieldId as itemId).from(right).where(notEmpty(itemId))
        )
        .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql should be(
      s"SELECT item_id FROM (SELECT item_id FROM join_query.twoTestTable as l1 CROSS JOIN (SELECT * FROM join_query.threeTestTable) AS r1) FORMAT JSON"
    )
  }
}
