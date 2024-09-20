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

  it should "tokenize IsNull" in {
    val expected = s"SELECT * FROM $database.twoTestTable WHERE isNull(uuid)"

    val query = select(All()).from(TwoTestTable).where(nativeUUID.isNull())
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(All()).from(TwoTestTable).where(isNull(nativeUUID))
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize IsNotNull" in {
    val expected = s"SELECT * FROM $database.twoTestTable WHERE isNotNull(uuid)"

    val query = select(All()).from(TwoTestTable).where(nativeUUID.isNotNull())
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(All()).from(TwoTestTable).where(isNotNull(nativeUUID))
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize IsNullable" in {
    val expected = s"SELECT isNullable(uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.isNullable()).from(TwoTestTable)
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(isNullable(nativeUUID)).from(TwoTestTable)
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize IsNotDistinctFrom" in {
    val expected = s"SELECT isNotDistinctFrom(uuid, uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.isNotDistinctFrom(nativeUUID)).from(TwoTestTable)
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(isNotDistinctFrom(nativeUUID, nativeUUID)).from(TwoTestTable)
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize IsZeroOrNull" in {
    val expected = s"SELECT isZeroOrNull(uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.isZeroOrNull()).from(TwoTestTable)
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(isZeroOrNull(nativeUUID)).from(TwoTestTable)
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize IfNull" in {
    val defaultValue = "alternative"
    val expected     = s"SELECT ifNull(uuid, '$defaultValue') FROM $database.twoTestTable"

    val query = select(nativeUUID.ifNull(defaultValue)).from(TwoTestTable)
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(ifNull(nativeUUID, defaultValue)).from(TwoTestTable)
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize NullIf" in {
    val expected = s"SELECT nullIf(uuid, uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.nullIf(nativeUUID)).from(TwoTestTable)
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(nullIf(nativeUUID, nativeUUID)).from(TwoTestTable)
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize AssumeNotNull" in {
    val expected = s"SELECT assumeNotNull(uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.assumeNotNull()).from(TwoTestTable)
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(assumeNotNull(nativeUUID)).from(TwoTestTable)
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

  it should "tokenize ToNullable" in {
    val expected = s"SELECT toNullable(uuid) FROM $database.twoTestTable"

    val query    = select(nativeUUID.toNullable()).from(TwoTestTable)
    toSql(query.internalQuery, None) should matchSQL(expected)

    val query2 = select(toNullable(nativeUUID)).from(TwoTestTable)
    toSql(query2.internalQuery, None) should matchSQL(expected)
  }

}
