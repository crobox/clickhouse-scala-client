package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.DslITSpec.IntResult
import com.crobox.clickhouse.dsl.JoinQuery.{AllLeftJoin, InnerJoin}

import java.util.UUID

class JoinExpressionOnIT extends DslITSpec {

  override val table1Entries: Seq[Table1Entry] = Seq(
    Table1Entry(UUID.randomUUID(), numbers = Seq(1, 2, 3))
  )
  override val table2Entries: Seq[Table2Entry] = Seq(
    Table2Entry(UUID.randomUUID(), "a", 1, "x", None),
    Table2Entry(UUID.randomUUID(), "b", 3, "y", None)
  )

  // ClickHouse < 24 rejects `arrayJoin(...)` in `JOIN ON` with INVALID_JOIN_ON_EXPRESSION (Code 403).
  it should "join with arrayJoin(leftCol(...)) on an expression LHS (INNER JOIN)" in {
    assumeMinimalClickhouseVersion(24)
    val query =
      select(col2)
        .from(OneTestTable)
        .join(InnerJoin, TwoTestTable)
        .on((arrayJoin(leftCol(numbers)), "=", col2))
    val resultRows = queryExecutor.execute[IntResult](query).futureValue.rows
    resultRows.length shouldBe 2
  }

  // ALL LEFT JOIN keeps unmatched arrayJoin'd left rows → 3 (matches 1 and 3, plus unmatched 2).
  it should "join with arrayJoin(leftCol(...)) on an expression LHS (ALL LEFT JOIN)" in {
    assumeMinimalClickhouseVersion(24)
    val query =
      select(col2)
        .from(OneTestTable)
        .join(AllLeftJoin, TwoTestTable)
        .on((arrayJoin(leftCol(numbers)), "=", col2))
    val resultRows = queryExecutor.execute[IntResult](query).futureValue.rows
    resultRows.length shouldBe 3
  }
}
