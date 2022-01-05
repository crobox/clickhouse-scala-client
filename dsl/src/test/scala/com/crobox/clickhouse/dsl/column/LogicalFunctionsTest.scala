package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class LogicalFunctionsTest extends DslTestSpec {
  val source: TableColumn[Boolean]    = shieldId.isEq("a")
  val condition: TableColumn[Boolean] = shieldId.isEq("b")

  it should "AND conditions" in {
    toSQL(source.and(Option(condition))) should matchSQL("shield_id = 'a' AND shield_id = 'b'")
    toSQL(source.and(None)) should matchSQL("shield_id = 'a'")

    toSQL(Option(source).and(Option(condition))) should matchSQL("shield_id = 'a' AND shield_id = 'b'")
    toSQL(Option(source).and(None)) should matchSQL("shield_id = 'a'")
    toSQL(None.and(None)) should matchSQL("1")
    toSQL(None.and(Option(condition))) should matchSQL("shield_id = 'b'")
  }

  it should "OR conditions" in {
    toSQL(source.or(Option(condition))) should matchSQL("shield_id = 'a' OR shield_id = 'b'")
    toSQL(source.or(None)) should matchSQL("shield_id = 'a'")

    toSQL(Option(source).or(Option(condition))) should matchSQL("shield_id = 'a' OR shield_id = 'b'")
    toSQL(Option(source).or(None)) should matchSQL("shield_id = 'a'")
    toSQL(None.or(None)) should matchSQL("1")
    toSQL(None.or(Option(condition))) should matchSQL("shield_id = 'b'")
  }

  it should "XOR conditions" in {
    toSQL(source.xor(Option(condition))) should matchSQL("xor(shield_id = 'a', shield_id = 'b')")
    toSQL(source.xor(None)) should matchSQL("shield_id = 'a'")

    toSQL(Option(source).xor(Option(condition))) should matchSQL("xor(shield_id = 'a', shield_id = 'b')")
    toSQL(Option(source).xor(None)) should matchSQL("shield_id = 'a'")
    toSQL(None.xor(None)) should matchSQL("1")
    toSQL(None.xor(Option(condition))) should matchSQL("shield_id = 'b'")
  }
}
