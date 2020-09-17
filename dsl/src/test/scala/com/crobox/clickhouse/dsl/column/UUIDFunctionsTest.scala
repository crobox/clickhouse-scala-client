package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._

class UUIDFunctionsTest extends ColumnFunctionTest {

  it should "rewrite empty to empty(0)" in {
    var query = select(All()).from(TwoTestTable).where(nativeUUID.empty())
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE uuid == 0"
    )
  }

  it should "rewrite notEmpty to notEquals(0)" in {
    var query = select(All()).from(TwoTestTable).where(nativeUUID.notEmpty())
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE uuid != 0"
    )

    query = select(All()).from(TwoTestTable).where(notEmpty(nativeUUID))
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE uuid != 0"
    )
  }
}
