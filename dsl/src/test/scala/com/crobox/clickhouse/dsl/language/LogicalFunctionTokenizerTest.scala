package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class LogicalFunctionTokenizerTest extends DslTestSpec {

  def noto(other: LogicalOpsMagnet): ExpressionColumn[Boolean] = LogicalFunction(other, Not, other)

  val select = SelectQuery(Seq(shieldId))

  it should "add brackets (not operator and OR)" in {
    toSQL(noto(shieldId isEq "a") or noto(shieldId isEq "b")) should matchSQL(
      "not(shield_id = 'a') OR not(shield_id = 'b')"
    )

    // explicit double quotes
    toSQL((noto(shieldId isEq "a")) or (noto(shieldId isEq "b"))) should matchSQL(
      s"not(shield_id = 'a') OR not(shield_id = 'b')"
    )
  }

  it should "add brackets (not operator and AND)" in {
    toSQL(noto(shieldId isEq "a") and noto(shieldId isEq "b")) should matchSQL(
      s"not(shield_id = 'a') AND not(shield_id = 'b')"
    )
    // explicit double quotes
    toSQL((noto(shieldId isEq "a")) and (noto(shieldId isEq "b"))) should matchSQL(
      s"not(shield_id = 'a') AND not(shield_id = 'b')"
    )
  }

  it should "add brackets (OR and AND)" in {
    toSQL(shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c"))) should matchSQL(
      s"shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c')"
    )
  }

  it should "add brackets (AND and OR)" in {
    toSQL(shieldId isEq "a" and ((shieldId isEq "b") or (shieldId isEq "c"))) should matchSQL(
      s"shield_id = 'a' AND (shield_id = 'b' OR shield_id = 'c')"
    )
  }

  it should "add brackets nested OR (left double, right single)" in {
    toSQL((shieldId < "a" and (shieldId isEq "b")) or shieldId < "c") should matchSQL(
      s"(shield_id < 'a' AND shield_id = 'b') OR shield_id < 'c'"
    )
  }

  it should "add brackets nested AND (left double, right single)" in {
    toSQL((shieldId < "a" and (shieldId isEq "b")) and shieldId < "c") should matchSQL(
      s"shield_id < 'a' AND shield_id = 'b' AND shield_id < 'c'"
    )
  }

  it should "add brackets nested OR (left double, right double)" in {
    toSQL((shieldId < "a" and (shieldId isEq "b")) or (shieldId < "c" or shieldId > "d")) should matchSQL(
      s"(shield_id < 'a' AND shield_id = 'b') OR shield_id < 'c' OR shield_id > 'd'"
    )
  }

  it should "add brackets nested AND (left double, right double)" in {
    toSQL((shieldId < "a" and (shieldId isEq "b")) and (shieldId < "c" or shieldId > "d")) should matchSQL(
      s"shield_id < 'a' AND shield_id = 'b' AND (shield_id < 'c' OR shield_id > 'd')"
    )
  }

  it should "add brackets NOT (single function)" in {
    toSQL(
      shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c")) or (noto(shieldId isEq "d") and noto(
        shieldId isEq "e"
      ))
    ) should matchSQL(
      s"shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c') OR (not(shield_id = 'd') AND not(shield_id = 'e'))"
    )
  }

  it should "add brackets triple OR/OR/OR" in {
    toSQL(shieldId isEq "a" or ((shieldId isEq "b") or (shieldId isEq "c") or (shieldId isEq "d"))) should matchSQL(
      "shield_id = 'a' OR shield_id = 'b' OR shield_id = 'c' OR shield_id = 'd'"
    )
  }

  it should "add brackets triple OR/AND/AND" in {
    toSQL(shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c") and (shieldId isEq "d"))) should matchSQL(
      s"shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c' AND shield_id = 'd')"
    )
  }

  it should "add brackets triple OR/AND/OR" in {
    toSQL(shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c") or (shieldId isEq "d"))) should matchSQL(
      s"shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c') OR shield_id = 'd'"
    )
  }

  it should "add brackets triple AND/AND/OR" in {
    toSQL(shieldId isEq "a" and ((shieldId isEq "b") and (shieldId isEq "c") or (shieldId isEq "d"))) should matchSQL(
      s"shield_id = 'a' AND ((shield_id = 'b' AND shield_id = 'c') OR shield_id = 'd')"
    )
  }

  it should "add brackets triple OR/OR/AND" in {
    toSQL(shieldId isEq "a" or ((shieldId isEq "b") or (shieldId isEq "c") and (shieldId isEq "d"))) should matchSQL(
      s"shield_id = 'a' OR ((shield_id = 'b' OR shield_id = 'c') AND shield_id = 'd')"
    )
  }

  def conditionOr(nr: Seq[Int]): Option[TableColumn[Boolean]] = Option(
    nr.map(x => col2 isEq x).reduce((a, b) => a or b)
  )

  def conditionAnd(nr: Seq[Int]): Option[TableColumn[Boolean]] =
    Option(nr.map(x => col2 isEq x).reduce((a, b) => a and b))

  it should "tokenize numbers OR with NONE" in {
    toSQL(None and conditionOr(Seq(1, 3)) and None and conditionOr(Seq(3, 4)) and None) should matchSQL(
      s"(column_2 = 1 OR column_2 = 3) AND (column_2 = 3 OR column_2 = 4)"
    )
  }

  //
  // OR
  //

  it should "true using Multiple values and/None/and OR" in {
    toSQL((1 == 1) and None and conditionOr(Seq(2, 3))) should matchSQL("column_2 = 2 OR column_2 = 3")
  }

  it should "true using Multiple values and/None/or OR" in {
    toSQL((1 == 1) and None or conditionOr(Seq(2, 3))) should matchSQL("1")
  }

  it should "true using Multiple values and/None/xor OR" in {
    toSQL((1 == 1) and None xor conditionOr(Seq(2, 3))) should matchSQL("not(column_2 = 2 OR column_2 = 3)")
  }

  it should "true using Multiple values or/None/or OR" in {
    toSQL((1 == 1) or None or conditionOr(Seq(2, 3))) should matchSQL("1")
  }

  it should "true using Multiple values or/None/and OR" in {
    toSQL((1 == 1) or None and conditionOr(Seq(2, 3))) should matchSQL("column_2 = 2 OR column_2 = 3")
  }

  it should "true using Multiple values or/None/xor OR" in {
    toSQL((1 == 1) or None xor conditionOr(Seq(2, 3))) should matchSQL("not(column_2 = 2 OR column_2 = 3)")
  }

  it should "false using Multiple values and/None/and OR" in {
    toSQL((1 == 2) and None and conditionOr(Seq(2, 3))) should matchSQL("0")
  }

  it should "false using Multiple values and/None/or OR" in {
    toSQL((1 == 2) and None or conditionOr(Seq(2, 3))) should matchSQL("column_2 = 2 OR column_2 = 3")
  }

  it should "false using Multiple values and/None/xor OR" in {
    toSQL((1 == 2) and None xor conditionOr(Seq(2, 3))) should matchSQL("column_2 = 2 OR column_2 = 3")
  }

  it should "false using Multiple values or/None/or OR" in {
    toSQL((1 == 2) or None or conditionOr(Seq(2, 3))) should matchSQL("column_2 = 2 OR column_2 = 3")
  }

  it should "false using Multiple values or/None/and OR" in {
    toSQL((1 == 2) or None and conditionOr(Seq(2, 3))) should matchSQL("0")
  }

  it should "false using Multiple values or/None/xor OR" in {
    toSQL((1 == 2) or None xor conditionOr(Seq(2, 3))) should matchSQL("column_2 = 2 OR column_2 = 3")
  }

  //
  // AND
  //

  it should "true using Multiple values and/None/and AND" in {
    toSQL((1 == 1) and None and conditionAnd(Seq(2, 3))) should matchSQL("column_2 = 2 AND column_2 = 3")
  }

  it should "true using Multiple values and/None/or AND" in {
    toSQL((1 == 1) and None or conditionAnd(Seq(2, 3))) should matchSQL("1")
  }

  it should "true using Multiple values and/None/xor AND" in {
    toSQL((1 == 1) and None xor conditionAnd(Seq(2, 3))) should matchSQL("not(column_2 = 2 AND column_2 = 3)")
  }

  it should "true using Multiple values or/None/or AND" in {
    toSQL((1 == 1) or None or conditionAnd(Seq(2, 3))) should matchSQL("1")
  }

  it should "true using Multiple values or/None/and AND" in {
    toSQL((1 == 1) or None and conditionAnd(Seq(2, 3))) should matchSQL("column_2 = 2 AND column_2 = 3")
  }

  it should "true using Multiple values or/None/xor AND" in {
    toSQL((1 == 1) or None xor conditionAnd(Seq(2, 3))) should matchSQL("not(column_2 = 2 AND column_2 = 3)")
  }

  it should "false using Multiple values and/None/and AND" in {
    toSQL((1 == 2) and None and conditionAnd(Seq(2, 3))) should matchSQL("0")
  }

  it should "false using Multiple values and/None/or AND" in {
    toSQL((1 == 2) and None or conditionAnd(Seq(2, 3))) should matchSQL("column_2 = 2 AND column_2 = 3")
  }

  it should "false using Multiple values and/None/xor AND" in {
    toSQL((1 == 2) and None xor conditionAnd(Seq(2, 3))) should matchSQL("column_2 = 2 AND column_2 = 3")
  }

  it should "false using Multiple values or/None/or AND" in {
    toSQL((1 == 2) or None or conditionAnd(Seq(2, 3))) should matchSQL("column_2 = 2 AND column_2 = 3")
  }

  it should "false using Multiple values or/None/and AND" in {
    toSQL((1 == 2) or None and conditionAnd(Seq(2, 3))) should matchSQL("0")
  }

  it should "false using Multiple values or/None/xor AND" in {
    toSQL((1 == 2) or None xor conditionAnd(Seq(2, 3))) should matchSQL("column_2 = 2 AND column_2 = 3")
  }

  it should "maintain brackets 1" in {
    toSQL(shieldId.isEq("a") and None.and(None).and(shieldId.isEq("b") or shieldId.isEq("c"))) should matchSQL(
      "shield_id = 'a' AND (shield_id = 'b' OR shield_id = 'c')"
    )
  }

  it should "maintain brackets 2" in {
    toSQL(None.and(None).and(shieldId.isEq("b") or shieldId.isEq("c"))) should matchSQL(
      "shield_id = 'b' OR shield_id = 'c'"
    )
  }

  it should "handle deeply nested OR without stack overflow" in {
    // This test would have caused StackOverflowError before the fix
    // Creates a deeply nested structure: (a OR (b OR (c OR (d OR ...))))
    val lotsOfConditions = (1 to 2000).map(i => col2 isEq i)
    val nestedOr         = lotsOfConditions.reduce[ExpressionColumn[Boolean]](_ or _)

    val sql = toSQL(nestedOr)
    sql should include("column_2 = 1")
    sql should include("column_2 = 100")
    sql should include(" OR ")
  }

  it should "handle deeply nested AND without stack overflow" in {
    // This test would have caused StackOverflowError before the fix
    // Creates a deeply nested structure: (a AND (b AND (c AND (d AND ...))))
    val lotsOfConditions = (1 to 2000).map(i => col2 isEq i)
    val nestedAnd        = lotsOfConditions.reduce[ExpressionColumn[Boolean]](_ and _)

    val sql = toSQL(nestedAnd)
    sql should include("column_2 = 1")
    sql should include("column_2 = 100")
    sql should include(" AND ")
  }

  it should "handle deeply nested mixed operators without stack overflow" in {
    // This test verifies mixed operators also work with deep nesting
    val conditions    = (1 to 1000).map(i => col2 isEq i)
    val orConditions  = conditions.take(500).reduce[ExpressionColumn[Boolean]](_ or _)
    val andConditions = conditions.drop(500).reduce[ExpressionColumn[Boolean]](_ and _)
    val mixed         = orConditions and andConditions

    val sql = toSQL(mixed)
    sql should include("column_2 = 1")
    sql should include("column_2 = 50")
    sql should include(" OR ")
    sql should include(" AND ")
  }
}
