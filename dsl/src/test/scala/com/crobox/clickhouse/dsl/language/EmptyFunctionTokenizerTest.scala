package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{dsl, DslTestSpec}

class EmptyFunctionTokenizerTest extends DslTestSpec {

  it should "UUID empty" in {
    if (ClickHouseVersion.minimalVersion(21, 8)) {
      toSQL(dsl.empty(shieldId)) should matchSQL("empty(shield_id)")
    } else {
      toSQL(dsl.empty(shieldId)) should matchSQL("shield_id == 0")
    }
  }

  it should "UUID notEmpty" in {
    if (ClickHouseVersion.minimalVersion(21, 8)) {
      toSQL(dsl.notEmpty(shieldId)) should matchSQL("notEmpty(shield_id)")
    } else {
      toSQL(dsl.notEmpty(shieldId)) should matchSQL("shield_id != 0")
    }
  }
}
