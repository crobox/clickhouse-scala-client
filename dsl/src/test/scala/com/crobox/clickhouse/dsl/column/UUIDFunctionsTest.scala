package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class UUIDFunctionsTest extends DslTestSpec {

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
