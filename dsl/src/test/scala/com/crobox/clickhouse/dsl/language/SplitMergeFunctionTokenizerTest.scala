package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{dsl, DslTestSpec}

class SplitMergeFunctionTokenizerTest extends DslTestSpec {
  val FIELD_DELIMITER: Char = '\u001F'

  it should "splitByChar using special character" in {

    toSQL(select(dsl.splitByChar(FIELD_DELIMITER.toString, "abcd"))) should matchSQL(
      "SELECT splitByChar(char(31),'abcd')"
    )
    toSQL(select(dsl.splitByChar(31.toChar.toString, "abcd"))) should matchSQL("SELECT splitByChar(char(31),'abcd')")
    toSQL(select(dsl.splitByChar(32.toChar.toString, "abcd"))) should matchSQL("SELECT splitByChar(' ','abcd')")
    toSQL(select(dsl.splitByChar(126.toChar.toString, "abcd"))) should matchSQL("SELECT splitByChar('~','abcd')")
    toSQL(select(dsl.splitByChar(127.toChar.toString, "abcd"))) should matchSQL("SELECT splitByChar(char(127),'abcd')")
    toSQL(select(dsl.splitByChar(128.toChar.toString, "abcd"))) should matchSQL("SELECT splitByChar(char(128),'abcd')")
    toSQL(select(dsl.splitByChar(159.toChar.toString, "abcd"))) should matchSQL("SELECT splitByChar(char(159),'abcd')")

    toSQL(select(dsl.splitByChar(",", "abcd"))) should matchSQL("SELECT splitByChar(',','abcd')")
    toSQL(select(dsl.splitByChar("a", "abcd"))) should matchSQL("SELECT splitByChar('a','abcd')")
    toSQL(select(dsl.splitByChar("ab", "abcd"))) should matchSQL("SELECT splitByString('ab','abcd')")
    toSQL(select(dsl.splitByChar("L", "abcd"))) should matchSQL("SELECT splitByChar('L','abcd')")
    toSQL(select(dsl.splitByChar("$", "abcd"))) should matchSQL("SELECT splitByChar('$','abcd')")
    toSQL(select(dsl.splitByChar("-", "abcd"))) should matchSQL("SELECT splitByChar('-','abcd')")
  }
}
