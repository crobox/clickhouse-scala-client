package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslTestSpec

import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl._

class INFunctionsTest extends DslTestSpec {

  it should "use tableAlias for IN" in {
    toSQL(shieldId.in(select(itemId).from(OneTestTable).where(itemId.===("a")))) should matchSQL(s"""
        |shield_id IN (SELECT item_id FROM ${OneTestTable.quoted} AS T1 WHERE item_id = 'a')
        |""".stripMargin)
  }

  it should "use tableAlias for NOT IN" in {
    toSQL(shieldId.notIn(select(itemId).from(OneTestTable).where(itemId.===("a")))) should matchSQL(s"""
         |shield_id NOT IN (SELECT item_id FROM ${OneTestTable.quoted} AS T1 WHERE item_id = 'a')
         |""".stripMargin)
  }

  it should "SKIP tableAlias for IN as select clause" in {
    toSQL(select(itemId).from(OneTestTable).where(itemId.in(Seq("a", "b"))), false) should matchSQL(s"""
         |SELECT item_id FROM ${OneTestTable.quoted} WHERE item_id IN ('a', 'b')
         |""".stripMargin)

    toSQL(select(itemId.in(Seq("a", "b")) as "l").from(OneTestTable).where(itemId.===("a")), false) should matchSQL(s"""
         |SELECT item_id IN ('a', 'b') AS l FROM ${OneTestTable.quoted} WHERE item_id = 'a'
         |""".stripMargin)
  }

  it should "SKIP tableAlias for GLOBAL IN" in {
    toSQL(shieldId.globalIn(select(itemId).from(OneTestTable).where(itemId.===("a")))) should matchSQL(s"""
         |shield_id GLOBAL IN (SELECT item_id FROM ${OneTestTable.quoted} WHERE item_id = 'a')
         |""".stripMargin)
  }

  it should "SKIP tableAlias for GLOBAL NOT IN" in {
    toSQL(shieldId.globalNotIn(select(itemId).from(OneTestTable).where(itemId.===("a")))) should matchSQL(s"""
         |shield_id GLOBAL NOT IN (SELECT item_id FROM ${OneTestTable.quoted} WHERE item_id = 'a')
         |""".stripMargin)
  }

  it should "use tableAlias for nested IN" in {
    toSQL(
      shieldId.in(select(itemId).from(OneTestTable).join(InnerJoin, select(itemId).from(TwoTestTable)).using(itemId))
    ) should matchSQL(
      s"""
         |shield_id IN (
         |    SELECT item_id FROM ${OneTestTable.quoted} AS L1
         |    INNER JOIN (SELECT item_id FROM ${TwoTestTable.quoted}) AS R1 USING item_id)
         |""".stripMargin
    )
  }

  it should "use tableAlias for IN multiple tables" in {
    toSQL(
      (
        select(col4)
          .from(TwoTestTable)
          .where(
            col4.in(select(col4).from(ThreeTestTable)) and
            col2.in(select(col2).from(TwoTestTable)) and
            col2.in(select(col4).from(ThreeTestTable))
          )
        )
    ) should matchSQL(
      s"""
         |WHERE column_4 IN (SELECT column_4 FROM ${ThreeTestTable.quoted} AS T1)
         |  AND column_2 IN (SELECT column_2 FROM ${TwoTestTable.quoted} AS T2)
         |  AND column_2 IN (SELECT column_4 FROM ${ThreeTestTable.quoted} AS T1)
         |""".stripMargin
    )
  }
}
