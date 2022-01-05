package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl.{notEmpty, select}

class UUIDFunctionsIT extends DslITSpec {

  it should "handle notEmpty" in {
    val resultRows =
      chExecutor
        .execute[Result](select(shieldId as "result").from(OneTestTable).where(notEmpty(shieldId)))
        .futureValue
        .rows
    resultRows.length shouldBe 0
  }
}
