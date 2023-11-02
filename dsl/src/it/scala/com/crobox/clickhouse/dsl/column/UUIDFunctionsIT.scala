package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec.StringResult
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{dsl, DslITSpec}

import java.util.UUID

class UUIDFunctionsIT extends DslITSpec {
  override val table1Entries: Seq[Table1Entry] =
    Seq(Table1Entry(UUID.randomUUID()), Table1Entry(UUID.fromString("00000000-0000-0000-0000-000000000000")))

  it should "handle notEmpty" in {
    val resultRows =
      queryExecutor
        .execute[StringResult](select(shieldId as "result").from(OneTestTable).where(dsl.notEmpty(shieldId)))
        .futureValue
        .rows
    resultRows.length shouldBe 2
  }

  it should "handle toUUID" in {
    assumeMinimalClickhouseVersion(21, 8)
    r(dsl.empty(toUUID("00000000-0000-0000-0000-000000000000")) as "result") shouldBe "1"
    r(toUUID("00000000-0000-0000-0000-000000000000")) shouldBe "00000000-0000-0000-0000-000000000000"
  }

  it should "handle toUUIDOrZero" in {
    r(toUUIDOrZero("00000000-0000-0000-0000-000000000000")) shouldBe "00000000-0000-0000-0000-000000000000"
    r(toUUIDOrZero("0")) shouldBe "00000000-0000-0000-0000-000000000000"
    r(toUUIDOrZero("sdf")) shouldBe "00000000-0000-0000-0000-000000000000"
  }

  it should "handle toUUIDOrNull" in {
    r(toUUIDOrNull("00000000-0000-0000-0000-000000000000")) shouldBe "00000000-0000-0000-0000-000000000000"
    r(toUUIDOrNull("0")) shouldBe "\\N"
    r(toUUIDOrNull("sdf")) shouldBe "\\N"
  }
}
