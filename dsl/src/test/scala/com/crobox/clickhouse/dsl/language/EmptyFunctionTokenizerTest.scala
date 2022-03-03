package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{dsl, DslTestSpec}

class EmptyFunctionTokenizerTest extends DslTestSpec {

  it should "UUID empty" in {
    if (ClickHouseVersion.minimalVersion(21, 8)) {
      toSQL(dsl.empty(nativeUUID)) should matchSQL("empty(uuid)")
    } else {
      toSQL(dsl.empty(nativeUUID)) should matchSQL("uuid == 0")
    }
  }

  it should "UUID notEmpty" in {
    if (ClickHouseVersion.minimalVersion(21, 8)) {
      toSQL(dsl.notEmpty(nativeUUID)) should matchSQL("notEmpty(uuid)")
    } else {
      toSQL(dsl.notEmpty(nativeUUID)) should matchSQL("uuid != 0")
    }
  }

  it should "rewrite empty to empty(0)" in {
    val query = select(All()).from(TwoTestTable).where(nativeUUID.empty())
    if (ClickHouseVersion.minimalVersion(21, 8)) {
      toSql(query.internalQuery, None) should matchSQL(
        s"SELECT * FROM $database.twoTestTable WHERE empty(uuid)"
      )
    } else {
      toSql(query.internalQuery, None) should matchSQL(
        s"SELECT * FROM $database.twoTestTable WHERE uuid == 0"
      )
    }
  }

  it should "rewrite notEmpty to notEquals(0)" in {
    var query = select(All()).from(TwoTestTable).where(nativeUUID.notEmpty())
    if (ClickHouseVersion.minimalVersion(21, 8)) {
      toSql(query.internalQuery, None) should matchSQL(
        s"SELECT * FROM $database.twoTestTable WHERE notEmpty(uuid)"
      )
    } else {
      toSql(query.internalQuery, None) should matchSQL(
        s"SELECT * FROM $database.twoTestTable WHERE uuid != 0"
      )
    }

    query = select(All()).from(TwoTestTable).where(notEmpty(nativeUUID))
    if (ClickHouseVersion.minimalVersion(21, 8)) {
      toSql(query.internalQuery, None) should matchSQL(
        s"SELECT * FROM $database.twoTestTable WHERE notEmpty(uuid)"
      )
    } else {
      toSql(query.internalQuery, None) should matchSQL(
        s"SELECT * FROM $database.twoTestTable WHERE uuid != 0"
      )
    }
  }
}
