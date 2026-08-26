package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

class FixedStringTypingTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  "IPv6" should "round trip" in {
    sql(select(iPv6NumToString(iPv6StringToNum("::1")))) should matchSQL(
      "SELECT IPv6NumToString(IPv6StringToNum('::1'))"
    )
  }

  it should "accept an explicit cast" in {
    sql(select(iPv6NumToString(toFixedString(col3, 16)))) should matchSQL(
      "SELECT IPv6NumToString(toFixedString(column_3,16))"
    )
  }

  // The failure this typing exists to prevent: the server answers ILLEGAL_TYPE_OF_ARGUMENT, so it has to be a compile
  // error rather than a query that renders and fails.
  it should "refuse a plain string" in {
    """select(iPv6NumToString("::1"))""" shouldNot typeCheck
  }

  it should "refuse a string column" in {
    """select(iPv6NumToString(col3))""" shouldNot typeCheck
  }

  "UUID" should "round trip" in {
    sql(select(uUIDNumToString(uUIDStringToNum(col3)))) should matchSQL(
      "SELECT UUIDNumToString(UUIDStringToNum(column_3))"
    )
  }

  it should "refuse a plain string" in {
    """select(uUIDNumToString(col3))""" shouldNot typeCheck
  }

  "hex" should "accept a FixedString, which is how the bytes get printed" in {
    sql(select(hex(iPv6StringToNum("::1")))) should matchSQL("SELECT hex(IPv6StringToNum('::1'))")
    sql(select(hex(uUIDStringToNum(col3)))) should matchSQL("SELECT hex(UUIDStringToNum(column_3))")
  }

  // IPv4StringToNum returns UInt32, not FixedString(16), so it stays numeric and IPv4NumToString stays numeric too.
  "IPv4" should "round trip through numbers" in {
    sql(select(iPv4NumToString(iPv4StringToNum("1.2.3.4")))) should matchSQL(
      "SELECT IPv4NumToString(IPv4StringToNum('1.2.3.4'))"
    )
  }
}
