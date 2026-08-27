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
    // ON, not USING: `item_id` here is only a SELECT-list alias for `shield_id`, and a USING key must resolve against
    // the left table expression. ClickHouse's analyzer (default since 24.3) rejects `USING item_id` on this shape with
    // UNKNOWN_IDENTIFIER, so the tokenizer resolves the alias and emits the equivalent ON condition.
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id AS item_id FROM ${OneTestTable.quoted} AS L1 " +
        s"INNER JOIN (SELECT * FROM ${TwoTestTable.quoted}) AS R1 ON L1.shield_id = R1.item_id " +
        s"WHERE notEmpty(item_id) FORMAT JSON"
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
        s"WHERE notEmpty(item_id)) AS R1 ON L1.shield_id = R1.item_id WHERE notEmpty(item_id) FORMAT JSON"
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
    an[IllegalArgumentException] shouldBe thrownBy(toSql(query.internalQuery))
  }

  it should s"fail on set on and using" in {
    val query: OperationalQuery =
      select(shieldId as itemId)
        .from(OneTestTable)
        .where(notEmpty(itemId))
        .join(InnerJoin, TwoTestTable) using itemId on itemId
    an[IllegalArgumentException] shouldBe thrownBy(toSql(query.internalQuery))
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

  // The join number used to be read back after the right-hand side had been tokenized, so a join nested on the right
  // advanced the counter and the outer join ended up sharing the inner join's aliases (L2/R2 at both levels).
  it should "number a join nested on the right side below the outer join" in {
    val inner = select(itemId, col2).from(TwoTestTable).join(InnerJoin, ThreeTestTable) using itemId
    val query = select(shieldId as itemId).from(OneTestTable).join(InnerJoin, inner) using itemId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id AS item_id FROM ${OneTestTable.quoted} AS L1 " +
        s"INNER JOIN (SELECT item_id, column_2 FROM ${TwoTestTable.quoted} AS L2 " +
        s"INNER JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R2 USING item_id) AS R1 " +
        s"ON L1.shield_id = R1.item_id FORMAT JSON"
    )
  }

  // A chain keys every join off the first table: the left alias is claimed once and reused while the right aliases
  // count up. Anything keyed off an earlier join's right-hand side has to nest, since a JoinCondition names a column
  // rather than the table it belongs to.
  it should "chain several joins at one level, keyed off the first table" in {
    val query = select(itemId)
      .from(TwoTestTable)
      .join(InnerJoin, ThreeTestTable)
      .on(itemId)
      .join(AllLeftJoin, OneTestTable)
      .on(itemId)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id FROM ${TwoTestTable.quoted} AS L1 " +
        s"INNER JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R1 ON L1.item_id = R1.item_id " +
        s"ALL LEFT JOIN (SELECT * FROM ${OneTestTable.quoted}) AS R2 ON L1.item_id = R2.item_id FORMAT JSON"
    )
  }

  it should "chain joins that use USING" in {
    val query = select(itemId).from(TwoTestTable).join(InnerJoin, ThreeTestTable).using(itemId)
    toSql(query.join(InnerJoin, TwoTestTable).using(itemId).internalQuery) should matchSQL(
      s"SELECT item_id FROM ${TwoTestTable.quoted} AS L1 " +
        s"INNER JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R1 USING item_id " +
        s"INNER JOIN (SELECT * FROM ${TwoTestTable.quoted}) AS R2 USING item_id FORMAT JSON"
    )
  }

  it should "attach USING and ON to the join most recently added" in {
    val query = select(itemId)
      .from(TwoTestTable)
      .join(InnerJoin, ThreeTestTable)
      .using(itemId)
      .join(InnerJoin, OneTestTable)
      .on(itemId)
    query.internalQuery.joins.map(j => (j.using.map(_.name), j.on.size)) shouldBe Seq(
      (Seq("item_id"), 0),
      (Seq.empty, 1)
    )
  }

  it should "keep ARRAY JOIN between the left alias and the first JOIN of a chain" in {
    val query = select(itemId)
      .from(TwoTestTable)
      .withArrayJoin(this.numbers)
      .join(InnerJoin, ThreeTestTable)
      .on(itemId)
      .join(InnerJoin, TwoTestTable)
      .on(itemId)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id FROM ${TwoTestTable.quoted} AS L1 ARRAY JOIN numbers " +
        s"INNER JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R1 ON L1.item_id = R1.item_id " +
        s"INNER JOIN (SELECT * FROM ${TwoTestTable.quoted}) AS R2 ON L1.item_id = R2.item_id FORMAT JSON"
    )
  }

  it should "carry GLOBAL per join rather than for the whole chain" in {
    val query = select(itemId)
      .from(TwoTestTable)
      .join(InnerJoin, ThreeTestTable)
      .on(itemId)
      .globalJoin(InnerJoin, OneTestTable)
      .on(itemId)
    toSql(query.internalQuery) should include("GLOBAL INNER JOIN")
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id FROM ${TwoTestTable.quoted} AS L1 " +
        s"INNER JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R1 ON L1.item_id = R1.item_id " +
        s"GLOBAL INNER JOIN (SELECT * FROM ${OneTestTable.quoted}) AS R2 ON L1.item_id = R2.item_id FORMAT JSON"
    )
  }

  it should "refuse USING or ON with no join to attach them to" in {
    an[IllegalArgumentException] should be thrownBy select(itemId).from(TwoTestTable).using(itemId)
    an[IllegalArgumentException] should be thrownBy select(itemId).from(TwoTestTable).on(itemId)
  }
}
