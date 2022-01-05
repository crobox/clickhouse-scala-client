package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.ClickhouseSQLSupport
import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule

class INFunctionsTest extends ColumnFunctionTest with ClickhouseTokenizerModule with ClickhouseSQLSupport {

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
         |shield_id IN (SELECT item_id FROM ${OneTestTable.quoted} AS T1 AS L1 INNER JOIN (SELECT item_id FROM ${TwoTestTable.quoted} AS T2) AS R1 USING item_id)
         |""".stripMargin
    )
  }
}
