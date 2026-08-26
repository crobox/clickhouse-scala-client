package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

class MagnetEqualityTest extends DslTestSpec {

  // The case from #71.
  it should "make two identically built conditions equal" in {
    val first  = (2 === 2).and(3 === 3)
    val second = (2 === 2).and(3 === 3)
    first shouldBe second
  }

  it should "agree on hashCode, so equal conditions share a bucket" in {
    (2 === 2).and(3 === 3).hashCode shouldBe (2 === 2).and(3 === 3).hashCode
  }

  it should "keep differing conditions unequal" in {
    (2 === 2).and(3 === 3) should not be (2 === 2).and(3 === 4)
  }

  // isEq, not ===, and bound to typed vals: ScalaTest also defines === and Scala 3 resolves the bare operator to
  // ScalaTest's, which compares a Column to an Int and yields false, so the assertion silently tested nothing.
  it should "compare column-based conditions" in {
    val one: ExpressionColumn[Boolean]      = col2 isEq 1
    val alsoOne: ExpressionColumn[Boolean]  = col2 isEq 1
    val two: ExpressionColumn[Boolean]      = col2 isEq 2
    val otherCol: ExpressionColumn[Boolean] = col4 isEq "1"

    one shouldBe alsoOne
    one.hashCode shouldBe alsoOne.hashCode
    one should not be two
    one should not be otherCol
  }

  it should "deduplicate equal expressions in a select list" in {
    SelectQuery(Seq(sum(col2))).addColumn(sum(col2)).columns should have size 1
  }

  it should "let a Set collapse equal conditions" in {
    Set((2 === 2).and(3 === 3), (2 === 2).and(3 === 3)) should have size 1
  }

  // Regression for the review on #357: these all leave `column` as EmptyColumn, so the inherited Magnet equality made
  // them indistinguishable and a select list silently dropped one.
  it should "tell IN over different tables apart" in {
    (col4 in OneTestTable) should not be (col4 in TwoTestTable)
  }

  it should "tell IN over different subqueries apart" in {
    (col4 in select(col4).from(TwoTestTable)) should not be (col4 in select(col4).from(ThreeTestTable))
  }

  it should "tell an IN over a table from an IN over a subquery" in {
    (col4 in OneTestTable) should not be (col4 in select(col4).from(OneTestTable))
  }

  it should "keep both when a select list holds IN over two tables" in {
    SelectQuery(Seq(col4 in OneTestTable)).addColumn(col4 in TwoTestTable).columns should have size 2
  }

  it should "still tell IN over different literal collections apart" in {
    (col4 in Seq("a")) should not be (col4 in Seq("b"))
  }

  // These are the case classes whose `column` is `this`. A concrete equals on Magnet suppressed the one the compiler
  // synthesises for a case class, so they inherited `column == that.column` -- `this == this` -- and every comparison
  // or hash blew the stack. Reaching one only took a dedup that compares by value.
  "A node whose column is itself" should "hash without recursing" in {
    intDiv(timestampColumn, 1000).hashCode()
    abs(col2).hashCode()
    (col2 + 1).hashCode()
    cast(col2, ColumnType.UInt32).hashCode()
    toFixedString(col3, 2).hashCode()
    succeed
  }

  it should "compare structurally rather than by identity" in {
    intDiv(timestampColumn, 1000) shouldBe intDiv(timestampColumn, 1000)
    abs(col2) shouldBe abs(col2)
    cast(col2, ColumnType.UInt32) shouldBe cast(col2, ColumnType.UInt32)
    toFixedString(col3, 2) shouldBe toFixedString(col3, 2)
  }

  it should "still tell different operands apart" in {
    intDiv(timestampColumn, 1000) should not be intDiv(timestampColumn, 500)
    abs(col2) should not be abs(timestampColumn)
    toFixedString(col3, 2) should not be toFixedString(col3, 4)
  }

  it should "survive a dedup that compares by value" in {
    val expr = toDateTime(intDiv(toUInt64rNull(timestampColumn), 1000))
    Seq[Column](expr).contains(expr) shouldBe true
    SelectQuery(Seq(expr)).addColumn(expr).columns should have size 1
  }
}
