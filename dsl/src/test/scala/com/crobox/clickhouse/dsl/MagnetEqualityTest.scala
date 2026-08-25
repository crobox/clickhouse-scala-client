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

  it should "compare column-based conditions" in {
    (col2 === 1) shouldBe (col2 === 1)
    (col2 === 1) should not be (col2 === 2)
    (col2 === 1) should not be (col4 === "1")
  }

  it should "deduplicate equal expressions in a select list" in {
    SelectQuery(Seq(sum(col2))).addColumn(sum(col2)).columns should have size 1
  }

  it should "let a Set collapse equal conditions" in {
    Set((2 === 2).and(3 === 3), (2 === 2).and(3 === 3)) should have size 1
  }
}
