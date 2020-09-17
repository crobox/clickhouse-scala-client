package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._

class UUIDFunctionTest extends ColumnFunctionTest {

  it should "rewrite notEmpty to notEquals(0)" in {
    var query = select(All()).from(OneTestTable).where(shieldId.notEmpty())
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.captainAmerica WHERE shield_id != '0'"
    )

    query = select(All()).from(OneTestTable).where(notEmpty(shieldId))
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.captainAmerica WHERE shield_id != '0'"
    )
  }
}
