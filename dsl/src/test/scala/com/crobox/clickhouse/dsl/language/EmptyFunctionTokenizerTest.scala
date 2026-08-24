package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{dsl, DslTestSpec}

class EmptyFunctionTokenizerTest extends DslTestSpec {

  it should "UUID empty" in {
    toSQL(dsl.empty(nativeUUID)) should matchSQL("empty(uuid)")
  }

  it should "UUID notEmpty" in {
    toSQL(dsl.notEmpty(nativeUUID)) should matchSQL("notEmpty(uuid)")
  }

  it should "tokenize empty as empty()" in {
    val query = select(All()).from(TwoTestTable).where(nativeUUID.empty())
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE empty(uuid)"
    )
  }

  it should "tokenize notEmpty as notEmpty()" in {
    val query = select(All()).from(TwoTestTable).where(nativeUUID.notEmpty())
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE notEmpty(uuid)"
    )

    val query2 = select(All()).from(TwoTestTable).where(notEmpty(nativeUUID))
    toSql(query2.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE notEmpty(uuid)"
    )
  }
}
