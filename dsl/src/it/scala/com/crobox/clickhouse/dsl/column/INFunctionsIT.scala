package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl.misc.LogicalOperatorImprovements.ExpressionColumnImpr
import com.crobox.clickhouse.dsl.select

import java.util.UUID

class INFunctionsIT extends DslITSpec {

  override val table2Entries: Seq[Table2Entry] =
    Seq(
      Table2Entry(UUID.randomUUID(), "a", randomInt, randomString, Option("a")),
      Table2Entry(UUID.randomUUID(), "b", randomInt, randomString, Option("b")),
      Table2Entry(UUID.randomUUID(), "c", randomInt, randomString, Option("c"))
    )
  override val table3Entries: Seq[Table3Entry] =
    Seq(
      Table3Entry(UUID.randomUUID(), 2, Option("a"), "a", "a"),
      Table3Entry(UUID.randomUUID(), 2, Option("a"), "b", "b"),
      Table3Entry(UUID.randomUUID(), 2, Option("b"), "c", "c")
    )

  it should "use tableAlias for IN, single table" in {
    execute(
      select(col4).from(TwoTestTable).where(col4.in(select(col4).from(ThreeTestTable)))
    ).futureValue should be("a\nb")
  }

  it should "use tableAlias for IN multiple tables" in {
    // check if syntax is correct
    execute(
      select(col4)
        .from(TwoTestTable)
        .where(
          col4.in(select(col4).from(ThreeTestTable)) and
          col2.in(select(col2).from(TwoTestTable)) and
          col2.in(select(col4).from(ThreeTestTable))
        )
    ).futureValue should be("")
  }
}
