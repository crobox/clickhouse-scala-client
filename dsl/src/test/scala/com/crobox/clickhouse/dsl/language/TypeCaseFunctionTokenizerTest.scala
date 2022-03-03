package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class TypeCaseFunctionTokenizerTest extends DslTestSpec {

  it should "succeed for UUID functions" in {
    toSQL(select(toUUID(const("00000000-0000-0000-0000-000000000000")))) shouldBe "SELECT toUUID('00000000-0000-0000-0000-000000000000')"
    toSQL(select(toUUIDOrZero(const("123")))) shouldBe "SELECT toUUIDOrZero('123')"
    toSQL(select(toUUIDOrNull(const("123")))) shouldBe "SELECT toUUIDOrNull('123')"
  }
}
