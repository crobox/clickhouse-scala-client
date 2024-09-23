package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{DslTestSpec, dsl}

class EmptyFunctionTokenizerTest extends DslTestSpec {

  it should "UUID empty" in {
    val result = toSQL(dsl.empty(nativeUUID))
    if (serverVersion.minimalVersion(21, 8)) {
      result should matchSQL("empty(uuid)")
    } else {
      result should matchSQL("uuid == 0")
    }
  }

  it should "UUID notEmpty" in {
    val result = toSQL(dsl.notEmpty(nativeUUID))
    if (serverVersion.minimalVersion(21, 8)) {
      result should matchSQL("notEmpty(uuid)")
    } else {
      result should matchSQL("uuid != 0")
    }
  }

  it should "rewrite empty to empty(0)" in {
    val query  = select(All()).from(TwoTestTable).where(nativeUUID.empty())
    val result = toSql(query.internalQuery, None)
    if (serverVersion.minimalVersion(21, 8)) {
      result should matchSQL(s"SELECT * FROM $database.twoTestTable WHERE empty(uuid)")
    } else {
      result should matchSQL(s"SELECT * FROM $database.twoTestTable WHERE uuid == 0")
    }
  }

  it should "rewrite notEmpty to notEquals(0)" in {
    val query  = select(All()).from(TwoTestTable).where(nativeUUID.notEmpty())
    val result = toSql(query.internalQuery, None)
    if (serverVersion.minimalVersion(21, 8)) {
      result should matchSQL(s"SELECT * FROM $database.twoTestTable WHERE notEmpty(uuid)")
    } else {
      result should matchSQL(s"SELECT * FROM $database.twoTestTable WHERE uuid != 0")
    }

    val query2  = select(All()).from(TwoTestTable).where(notEmpty(nativeUUID))
    val result2 = toSql(query2.internalQuery, None)
    if (serverVersion.minimalVersion(21, 8)) {
      result2 should matchSQL(s"SELECT * FROM $database.twoTestTable WHERE notEmpty(uuid)")
    } else {
      result2 should matchSQL(s"SELECT * FROM $database.twoTestTable WHERE uuid != 0")
    }
  }
}
