package com.crobox.clickhouse.dsl

import com.crobox.clickhouse._
import com.crobox.clickhouse.{dsl => CHDsl}
import java.util.UUID

class WithQueryTest extends DslTestSpec {

  it should "generate simple WITH expression" in {
    val query = withExpression("random_value", const(42))
      .select(CHDsl.all)
      .from(OneTestTable)

    toSql(query.internalQuery) should matchSQL(
      s"WITH random_value AS 42 SELECT * FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "generate WITH expression with column reference" in {
    val query = withExpression("shield_value", shieldId)
      .select(CHDsl.all)
      .from(OneTestTable)

    toSql(query.internalQuery) should matchSQL(
      s"WITH shield_value AS shield_id SELECT * FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "generate multiple WITH expressions" in {
    val query = withExpressions(
      "random_number" -> const(100),
      "shield_value" -> shieldId
    ).select(CHDsl.all).from(OneTestTable)

    toSql(query.internalQuery) should matchSQL(
      s"WITH random_number AS 100, shield_value AS shield_id SELECT * FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "generate WITH expression with function call" in {
    val query = withExpression("random_value", rand())
      .select(CHDsl.all)
      .from(OneTestTable)

    toSql(query.internalQuery) should matchSQL(
      s"WITH random_value AS rand() SELECT * FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "add WITH expression to existing query" in {
    val baseQuery = select(shieldId).from(OneTestTable)
    val queryWithWith = baseQuery.withExpression("constant_value", const(123))

    toSql(queryWithWith.internalQuery) should matchSQL(
      s"WITH constant_value AS 123 SELECT shield_id FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "support chaining WITH expressions" in {
    val query = withExpression("first_value", const(1))
      .withExpression("second_value", const(2))
      .select(CHDsl.all)
      .from(OneTestTable)

    toSql(query.internalQuery) should matchSQL(
      s"WITH first_value AS 1, second_value AS 2 SELECT * FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "generate WITH expression with subquery" in {
    val subquery = select(shieldId).from(OneTestTable).limit(Some(Limit(1)))
    val query = withExpression("max_shield", subquery.internalQuery.select.get.columns.head)
      .select(CHDsl.all)
      .from(TwoTestTable)

    toSql(query.internalQuery) should matchSQL(
      s"WITH max_shield AS shield_id SELECT * FROM $database.twoTestTable FORMAT JSON"
    )
  }

  it should "work with WHERE clause" in {
    val testUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    val query = withExpression("target_value", const(42))
      .select(CHDsl.all)
      .from(OneTestTable)
      .where(shieldId.isEq(testUuid))

    toSql(query.internalQuery) should matchSQL(
      s"WITH target_value AS 42 SELECT * FROM $database.captainAmerica WHERE shield_id = '550e8400-e29b-41d4-a716-446655440000' FORMAT JSON"
    )
  }

  it should "work with GROUP BY clause" in {
    val query = withExpression("constant_value", const(100))
      .select(shieldId, count())
      .from(OneTestTable)
      .groupBy(shieldId)

    toSql(query.internalQuery) should matchSQL(
      s"WITH constant_value AS 100 SELECT shield_id, count() FROM $database.captainAmerica GROUP BY shield_id FORMAT JSON"
    )
  }

  it should "work with ORDER BY clause" in {
    val query = withExpression("sort_value", const(1))
      .select(CHDsl.all)
      .from(OneTestTable)
      .orderBy(shieldId)

    toSql(query.internalQuery) should matchSQL(
      s"WITH sort_value AS 1 SELECT * FROM $database.captainAmerica ORDER BY shield_id ASC FORMAT JSON"
    )
  }

  it should "generate WITH clause with constant expression (variable)" in {
    // Example: WITH pi = 3.14159 SELECT sin(pi/2)
    val query = withExpression("pi", const(3.14159))
      .select(sin(ref[Double]("pi") / const(2)))
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    toSql(query.internalQuery) should matchSQL(
      s"WITH pi AS 3.14159 SELECT sin(pi / 2) FROM $database.captainAmerica LIMIT 0, 1 FORMAT JSON"
    )
  }

  it should "generate WITH clause with subquery CTE" in {
    // Example: WITH subset AS (SELECT shield_id FROM table LIMIT 5) SELECT count() FROM subset
    // Note: This demonstrates the subquery syntax, even though we can't directly reference the CTE in FROM with current DSL
    val subqueryExpression = raw(s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 5")
    
    val query = withSubquery("subset", subqueryExpression)
      .select(count())
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    toSql(query.internalQuery) should matchSQL(
      s"WITH subset AS (SELECT shield_id FROM $database.captainAmerica LIMIT 5) SELECT count() FROM $database.captainAmerica LIMIT 0, 1 FORMAT JSON"
    )
  }

  it should "generate WITH clause with scalar subquery result" in {
    // Example: WITH total_bytes = (SELECT sum(bytes) FROM table) SELECT ...
    val totalShields = select(count()).from(OneTestTable)
    
    val query = withExpression("total_shields", raw(s"(${toSql(totalShields.internalQuery, None)})"))
      .select(ref[Long]("total_shields"), shieldId)
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    toSql(query.internalQuery) should matchSQL(
      s"WITH total_shields AS (SELECT count() FROM $database.captainAmerica) SELECT total_shields, shield_id FROM $database.captainAmerica LIMIT 0, 1 FORMAT JSON"
    )
  }

  it should "generate WITH clause with multiple expressions" in {
    // Example combining multiple WITH expressions: constants and calculations
    val query = withExpressions(
      "base_value" -> const(100),
      "multiplier" -> const(2.5),
      "calculated" -> (ref[Int]("base_value") * ref[Double]("multiplier"))
    ).select(ref[Double]("calculated"), shieldId)
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    toSql(query.internalQuery) should matchSQL(
      s"WITH base_value AS 100, multiplier AS 2.5, calculated AS base_value * multiplier SELECT calculated, shield_id FROM $database.captainAmerica LIMIT 0, 1 FORMAT JSON"
    )
  }

  it should "generate WITH clause referencing column expressions" in {
    // Using WITH to create reusable column expressions
    val query = withExpression("shield_str", toStringRep(shieldId))
      .select(ref[String]("shield_str"), shieldId)
      .from(OneTestTable)
      .where(ref[String]("shield_str").length() > const(30))
      .limit(Some(Limit(1)))

    toSql(query.internalQuery) should matchSQL(
      s"WITH shield_str AS toString(shield_id) SELECT shield_str, shield_id FROM $database.captainAmerica WHERE length(shield_str) > 30 LIMIT 0, 1 FORMAT JSON"
    )
  }
}