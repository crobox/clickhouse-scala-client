package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{dsl, DslTestSpec}

class SplitMergeFunctionTokenizerTest extends DslTestSpec {
  val FIELD_DELIMITER: Char = '\u001F'

  it should "splitByChar using special character" in {

    toSQL(select(dsl.splitByChar(FIELD_DELIMITER.toString, "abcd"))) should matchSQL(
      "SELECT splitByChar(char(31), 'abcd')"
    )
    toSQL(select(dsl.splitByChar(31.toChar, "abcd"))) should matchSQL("SELECT splitByChar(char(31), 'abcd')")
    toSQL(select(dsl.splitByChar(32.toChar, "abcd"))) should matchSQL("SELECT splitByChar(' ', 'abcd')")
    toSQL(select(dsl.splitByChar(126.toChar, "abcd"))) should matchSQL("SELECT splitByChar('~', 'abcd')")
    toSQL(select(dsl.splitByChar(127.toChar, "abcd"))) should matchSQL("SELECT splitByChar(char(127), 'abcd')")
    toSQL(select(dsl.splitByChar(128.toChar, "abcd"))) should matchSQL("SELECT splitByChar(char(128), 'abcd')")
    toSQL(select(dsl.splitByChar(159.toChar, "abcd"))) should matchSQL("SELECT splitByChar(char(159), 'abcd')")

    toSQL(select(dsl.splitByChar(',', "abcd"))) should matchSQL("SELECT splitByChar(',', 'abcd')")
    toSQL(select(dsl.splitByChar('a', "abcd"))) should matchSQL("SELECT splitByChar('a', 'abcd')")
    toSQL(select(dsl.splitByChar('L', "abcd"))) should matchSQL("SELECT splitByChar('L', 'abcd')")
    toSQL(select(dsl.splitByChar('$', "abcd"))) should matchSQL("SELECT splitByChar('$', 'abcd')")
    toSQL(select(dsl.splitByChar('-', "abcd"))) should matchSQL("SELECT splitByChar('-', 'abcd')")
    toSQL(select(dsl.splitByChar('!', "abcd"))) should matchSQL("SELECT splitByChar('!', 'abcd')")

    // special 'not quoted' characters, see ClickhouseStatement.UnquotedIdentifier
    toSQL(select(dsl.splitByChar('\"', "abcd"))) should matchSQL("SELECT splitByChar(char(34), 'abcd')")
    toSQL(select(dsl.splitByChar('\'', "abcd"))) should matchSQL("SELECT splitByChar(char(39), 'abcd')")
    toSQL(select(dsl.splitByChar('`', "abcd"))) should matchSQL("SELECT splitByChar(char(96), 'abcd')")
    toSQL(select(dsl.splitByChar('\\', "abcd"))) should matchSQL("SELECT splitByChar(char(92), 'abcd')")
    toSQL(select(dsl.splitByChar('/', "abcd"))) should matchSQL("SELECT splitByChar('/', 'abcd')")

    // Multiple characters
    toSQL(select(dsl.splitByChar("ab", "abcd"))) should matchSQL("SELECT splitByString('ab', 'abcd')")
    toSQL(select(dsl.splitByChar("a'", "abcd"))) should matchSQL("SELECT splitByString('a\\'', 'abcd')")
  }
}
