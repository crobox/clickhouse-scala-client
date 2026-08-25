package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

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
}
