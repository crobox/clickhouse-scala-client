package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

import java.time.LocalDate

class DateSubtractionTest extends DslTestSpec {

  private val dateCol  = NativeColumn[LocalDate]("d")
  private val otherCol = NativeColumn[LocalDate]("d2")

  it should "subtract one date column from another" in {
    toSql(select(dateCol - otherCol).internalQuery, None) should matchSQL("SELECT d - d2")
  }

  // The literal goes through the DateOrDateTime magnet, which wraps it in toDate rather than passing a bare string.
  it should "subtract a date literal from a column" in {
    toSql(select(dateCol - LocalDate.of(2020, 1, 1)).internalQuery, None) should matchSQL(
      "SELECT d - toDate('2020-01-01')"
    )
  }

  it should "keep the existing date-plus-number arithmetic" in {
    toSql(select(dateCol + 1).internalQuery, None) should matchSQL("SELECT d + 1")
    toSql(select(dateCol - 1).internalQuery, None) should matchSQL("SELECT d - 1")
  }

  // The server rejects plus on two dates, so this must not typecheck.
  it should "refuse to add two dates" in {
    """select(dateCol + otherCol)""" shouldNot typeCheck
  }

  it should "keep numeric arithmetic working through the new evidence" in {
    toSql(select(col2 - 1).internalQuery, None) should matchSQL("SELECT column_2 - 1")
    toSql(select(col2 + 1).internalQuery, None) should matchSQL("SELECT column_2 + 1")
  }
}
