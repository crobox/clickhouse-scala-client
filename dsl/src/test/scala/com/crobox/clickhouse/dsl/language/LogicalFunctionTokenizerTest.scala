package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl._
import org.scalatest.Assertion

class LogicalFunctionTokenizerTest extends ClickhouseClientSpec with TestSchema with ClickhouseTokenizerModule {
  val testSubject = this
  val database    = "default"

  def noto(other: LogicalOpsMagnet): ExpressionColumn[Boolean] = LogicalFunction(other, Not, other)

  val select = SelectQuery(Seq(shieldId))

  it should "add brackets (not operator and OR)" in {
    testQuery(
      Some(noto(shieldId isEq "a") or noto(shieldId isEq "b")),
      s"WHERE not(shield_id = 'a') OR not(shield_id = 'b')"
    )
    // explicit double quotes
    testQuery(
      Some((noto(shieldId isEq "a")) or (noto(shieldId isEq "b"))),
      s"WHERE not(shield_id = 'a') OR not(shield_id = 'b')"
    )
  }

  it should "add brackets (not operator and AND)" in {
    testQuery(
      Some(noto(shieldId isEq "a") and noto(shieldId isEq "b")),
      s"WHERE not(shield_id = 'a') AND not(shield_id = 'b')"
    )
    // explicit double quotes
    testQuery(
      Some((noto(shieldId isEq "a")) and (noto(shieldId isEq "b"))),
      s"WHERE not(shield_id = 'a') AND not(shield_id = 'b')"
    )
  }

  it should "add brackets (OR and AND)" in {
    testQuery(
      Some(shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c"))),
      s"WHERE shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c')"
    )
  }

  it should "add brackets (AND and OR)" in {
    testQuery(
      Some(shieldId isEq "a" and ((shieldId isEq "b") or (shieldId isEq "c"))),
      s"WHERE shield_id = 'a' AND (shield_id = 'b' OR shield_id = 'c')"
    )
  }

  it should "add brackets nested OR (left double, right single)" in {
    testQuery(
      Some((shieldId < "a" and (shieldId isEq "b")) or shieldId < "c"),
      s"WHERE (shield_id < 'a' AND shield_id = 'b') OR shield_id < 'c'"
    )
  }

  it should "add brackets nested AND (left double, right single)" in {
    testQuery(
      Some((shieldId < "a" and (shieldId isEq "b")) and shieldId < "c"),
      s"WHERE shield_id < 'a' AND shield_id = 'b' AND shield_id < 'c'"
    )
  }

  it should "add brackets nested OR (left double, right double)" in {
    testQuery(
      Some((shieldId < "a" and (shieldId isEq "b")) or (shieldId < "c" or shieldId > "d")),
      //s"WHERE (shield_id < 'a' AND shield_id = 'b') OR (shield_id < 'c' OR shield_id > 'd')"
      s"WHERE (shield_id < 'a' AND shield_id = 'b') OR shield_id < 'c' OR shield_id > 'd'"
    )
  }

  it should "add brackets nested AND (left double, right double)" in {
    testQuery(
      Some((shieldId < "a" and (shieldId isEq "b")) and (shieldId < "c" or shieldId > "d")),
      s"WHERE shield_id < 'a' AND shield_id = 'b' AND (shield_id < 'c' OR shield_id > 'd')"
    )
  }

  it should "add brackets NOT (single function)" in {
    testQuery(
      Some(
        shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c")) or (noto(shieldId isEq "d") and noto(
          shieldId isEq "e"
        ))
      ),
      s"WHERE shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c') OR (not(shield_id = 'd') AND not(shield_id = 'e'))"
    )
  }

  it should "add brackets triple OR/OR/OR" in {
    testQuery(
      Some(shieldId isEq "a" or ((shieldId isEq "b") or (shieldId isEq "c") or (shieldId isEq "d"))),
      "WHERE shield_id = 'a' OR shield_id = 'b' OR shield_id = 'c' OR shield_id = 'd'"
    )
  }

  it should "add brackets triple OR/AND/AND" in {
    testQuery(
      Some(shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c") and (shieldId isEq "d"))),
      s"WHERE shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c' AND shield_id = 'd')"
    )
  }

  it should "add brackets triple OR/AND/OR" in {
    testQuery(
      Some(shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c") or (shieldId isEq "d"))),
      s"WHERE shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c') OR shield_id = 'd'"
    )
  }

  it should "add brackets triple AND/AND/OR" in {
    testQuery(
      Some(shieldId isEq "a" and ((shieldId isEq "b") and (shieldId isEq "c") or (shieldId isEq "d"))),
      s"WHERE shield_id = 'a' AND ((shield_id = 'b' AND shield_id = 'c') OR shield_id = 'd')"
    )
  }

  it should "add brackets triple OR/OR/AND" in {
    testQuery(
      Some(shieldId isEq "a" or ((shieldId isEq "b") or (shieldId isEq "c") and (shieldId isEq "d"))),
      s"WHERE shield_id = 'a' OR ((shield_id = 'b' OR shield_id = 'c') AND shield_id = 'd')"
    )
  }

  it should "tokenize numbers OR with NONE" in {
    def condition(nr: Seq[Int]): Option[TableColumn[Boolean]] = Option(nr.map(x => col2 === x).reduce((a, b) => a or b))
    testQuery(
      Some(None and condition(Seq(1, 3)) and None and condition(Seq(3, 4)) and None),
      s"WHERE (column_2 = 1 OR column_2 = 3) AND (column_2 = 3 OR column_2 = 4)"
    )
  }

  def testQuery(where: Option[TableColumn[Boolean]], expected: String): Assertion = {
    val select    = SelectQuery(Seq(shieldId))
    val iQuery    = InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), None, where)
    var generated = testSubject.toSql(iQuery)
    generated = generated.substring(generated.indexOf("WHERE "))
    generated = generated.substring(0, generated.indexOf(" FORMAT"))

    generated should be(expected)
  }

}
