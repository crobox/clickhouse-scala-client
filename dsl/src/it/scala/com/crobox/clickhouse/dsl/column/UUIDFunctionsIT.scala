package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{select, toUUID}
import com.crobox.clickhouse.{DslITSpec, dsl}

import java.util.UUID

class UUIDFunctionsIT extends DslITSpec {
  override val table1Entries: Seq[Table1Entry] =
    Seq(Table1Entry(UUID.randomUUID()), Table1Entry(UUID.fromString("00000000-0000-0000-0000-000000000000")))

  it should "handle notEmpty" in {
    val resultRows =
      chExecutor
        .execute[StringResult](select(shieldId as "result").from(OneTestTable).where(dsl.notEmpty(shieldId)))
        .futureValue
        .rows
    resultRows.length shouldBe 2
  }

  it should "handle notEmpty with 0" in {
    val resultRows =
      chExecutor
        .execute[IntResult](select(dsl.empty(toUUID("00000000-0000-0000-0000-000000000000")) as "result"))
        .futureValue
        .rows
    resultRows.length shouldBe 1
  }
}
