package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class NullableFunctionTokenizerTest extends DslTestSpec {

  it should "tokenize IsNull" in {
    val expected = s"SELECT * FROM $database.twoTestTable WHERE isNull(uuid)"

    val q1 = select(All()).from(TwoTestTable).where(nativeUUID.isNull())
    shouldMatch(q1, expected)

    val q2 = select(All()).from(TwoTestTable).where(isNull(nativeUUID))
    shouldMatch(q2, expected)
  }

  it should "tokenize IsNull for Constants" in {
    val expected = s"SELECT * FROM $database.twoTestTable WHERE isNull(1)"

    val q1 = select(All()).from(TwoTestTable).where(const(1).isNull())
    val q2 = select(All()).from(TwoTestTable).where(1.isNull())
    val q3 = select(All()).from(TwoTestTable).where(isNull(1))
    val q4 = select(All()).from(TwoTestTable).where(isNull(const(1)))

    Seq(q1, q2, q3, q4).foreach(q => shouldMatch(q, expected))
  }

  it should "tokenize IsNotNull" in {
    val expected = s"SELECT * FROM $database.twoTestTable WHERE isNotNull(uuid)"

    val query = select(All()).from(TwoTestTable).where(nativeUUID.isNotNull())
    shouldMatch(query, expected)

    val query2 = select(All()).from(TwoTestTable).where(isNotNull(nativeUUID))
    shouldMatch(query2, expected)
  }

  it should "tokenize IsNullable" in {
    val expected = s"SELECT isNullable(uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.isNullable()).from(TwoTestTable)
    shouldMatch(query, expected)

    val query2 = select(isNullable(nativeUUID)).from(TwoTestTable)
    shouldMatch(query2, expected)
  }

  it should "tokenize IsZeroOrNull" in {
    val expected = s"SELECT isZeroOrNull(uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.isZeroOrNull()).from(TwoTestTable)
    shouldMatch(query, expected)

    val query2 = select(isZeroOrNull(nativeUUID)).from(TwoTestTable)
    shouldMatch(query2, expected)
  }

  it should "tokenize IfNull" in {
    val defaultValue = "alternative"
    val expected     = s"SELECT ifNull(uuid, '$defaultValue') FROM $database.twoTestTable"

    val query = select(nativeUUID.ifNull(defaultValue)).from(TwoTestTable)
    shouldMatch(query, expected)

    val query2 = select(ifNull(nativeUUID, defaultValue)).from(TwoTestTable)
    shouldMatch(query2, expected)
  }

  it should "tokenize NullIf" in {
    val expected = s"SELECT nullIf(uuid, uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.nullIf(nativeUUID)).from(TwoTestTable)
    shouldMatch(query, expected)

    val query2 = select(nullIf(nativeUUID, nativeUUID)).from(TwoTestTable)
    shouldMatch(query2, expected)
  }

  it should "tokenize AssumeNotNull" in {
    val expected = s"SELECT assumeNotNull(uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.assumeNotNull()).from(TwoTestTable)
    shouldMatch(query, expected)

    val query2 = select(assumeNotNull(nativeUUID)).from(TwoTestTable)
    shouldMatch(query2, expected)
  }

  it should "tokenize ToNullable" in {
    val expected = s"SELECT toNullable(uuid) FROM $database.twoTestTable"

    val query = select(nativeUUID.toNullable()).from(TwoTestTable)
    shouldMatch(query, expected)

    val query2 = select(toNullable(nativeUUID)).from(TwoTestTable)
    shouldMatch(query2, expected)
  }

}
