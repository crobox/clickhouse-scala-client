package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery.{AllLeftJoin, InnerJoin}
import com.crobox.clickhouse.{dsl, DslTestSpec}
import org.scalatest.prop.TableDrivenPropertyChecks

class JoinQueryTest extends DslTestSpec with TableDrivenPropertyChecks {

  it should s"join correctly on: ${JoinQuery.CrossJoin}" in {
    val query: OperationalQuery =
      select(itemId).from(select(itemId).from(TwoTestTable).join(JoinQuery.CrossJoin, ThreeTestTable))
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id FROM (SELECT item_id FROM ${TwoTestTable.quoted} AS L1 " +
        s"CROSS JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R1) FORMAT JSON"
    )
  }

  it should s"TABLE - TABLE - using" in {
    val query: OperationalQuery =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(InnerJoin, TwoTestTable) using itemId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id AS item_id FROM ${OneTestTable.quoted} AS L1 " +
        s"INNER JOIN (SELECT * FROM ${TwoTestTable.quoted}) AS R1 USING item_id WHERE notEmpty(item_id) FORMAT JSON"
    )
  }

  it should s"TABLE - QUERY - using" in {
    val query =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id AS item_id FROM ${OneTestTable.quoted} AS L1 " +
        s"INNER JOIN (SELECT item_id, column_2 FROM ${TwoTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS R1 USING item_id WHERE notEmpty(item_id) FORMAT JSON"
    )
  }

  it should s"QUERY - TABLE - using" in {
    val query =
      select(dsl.all)
        .from(
          select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId))
        )
        .join(InnerJoin, TwoTestTable)
        .where(notEmpty(itemId)) using itemId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM ${OneTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS L1 INNER JOIN (SELECT * FROM ${TwoTestTable.quoted}) AS R1 " +
        s"USING item_id WHERE notEmpty(item_id) FORMAT JSON"
    )
  }

  it should s"QUERY - QUERY - using" in {
    val query =
      select(dsl.all)
        .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
        .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) using itemId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM ${OneTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS L1 INNER JOIN (SELECT item_id, column_2 FROM ${TwoTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS R1 USING item_id FORMAT JSON"
    )
  }

  // ON --> check prefix per ON condition
  it should s"QUERY - QUERY - on simple" in {
    val query =
      select(dsl.all)
        .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
        .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on itemId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM ${OneTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS L1 INNER JOIN (SELECT item_id, column_2 FROM ${TwoTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS R1 ON L1.item_id = R1.item_id FORMAT JSON"
    )
  }

  // ON --> check prefix per ON condition
  it should s"QUERY - QUERY - on complex" in {
    val query =
      select(dsl.all)
        .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
        .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on ((itemId, "<=", itemId))
    toSql(query.internalQuery) should matchSQL(
      s"SELECT * FROM (SELECT shield_id AS item_id FROM ${OneTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS L1 INNER JOIN (SELECT item_id, column_2 FROM ${TwoTestTable.quoted} " +
        s"WHERE notEmpty(item_id)) AS R1 ON L1.item_id <= R1.item_id FORMAT JSON"
    )
  }

  it should s"fail on empty on and using" in {
    val query: OperationalQuery =
      select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)).join(InnerJoin, TwoTestTable)
    an[AssertionError] shouldBe thrownBy(toSql(query.internalQuery))
  }

  it should s"fail on set on and using" in {
    val query: OperationalQuery =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(InnerJoin, TwoTestTable) using itemId on itemId
    an[AssertionError] shouldBe thrownBy(toSql(query.internalQuery))
  }

  it should s"triple complex join query" in {
    val query =
      select(dsl.all)
        .from(
          select(dsl.all)
            .from(select(shieldId as itemId).from(OneTestTable).where(notEmpty(itemId)))
            .join(InnerJoin, select(itemId, col2).from(TwoTestTable).where(notEmpty(itemId))) on itemId
        )
        .join(AllLeftJoin, ThreeTestTable)
        .on(itemId)
    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT *
         |FROM
         |  (SELECT *
         |   FROM
         |     (SELECT shield_id AS item_id
         |      FROM ${OneTestTable.quoted}
         |      WHERE notEmpty(item_id)) AS L1
         |   INNER JOIN
         |     (SELECT item_id,
         |             column_2
         |      FROM ${TwoTestTable.quoted}
         |      WHERE notEmpty(item_id)) AS R1 ON L1.item_id = R1.item_id) AS L2 ALL
         |LEFT JOIN
         |  (SELECT *
         |   FROM ${ThreeTestTable.quoted}) AS R2 ON L2.item_id = R2.item_id
         |FORMAT JSON""".stripMargin
    )
  }

  it should s"triple complex join query with custom aliases" in {
    val query =
      select(dsl.all)
        .from(
          select(dsl.all)
            .from(select(shieldId as itemId).from(OneTestTable).as("ott_alias").where(notEmpty(itemId)))
            .as("1_lEfT")
            .join(InnerJoin, select(itemId, col2).from(TwoTestTable).as("ttt.alias").where(notEmpty(itemId))) on itemId
        )
        .as("2_leFT")
        .join(AllLeftJoin, ThreeTestTable)
        .on(itemId)
    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT *
         |FROM
         |  (SELECT *
         |   FROM
         |     (SELECT shield_id AS item_id
         |      FROM ${OneTestTable.quoted} AS ott_alias
         |      WHERE notEmpty(item_id)) AS `1_lEfT`
         |   INNER JOIN
         |     (SELECT item_id,
         |             column_2
         |      FROM ${TwoTestTable.quoted} AS `ttt.alias`
         |      WHERE notEmpty(item_id)) AS R1 ON `1_lEfT`.item_id = R1.item_id) AS `2_leFT` ALL
         |LEFT JOIN
         |  (SELECT *
         |   FROM ${ThreeTestTable.quoted}) AS R2 ON `2_leFT`.item_id = R2.item_id
         |FORMAT JSON""".stripMargin
    )
  }
}
