package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}

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

  private val stamp      = NativeColumn[ZonedDateTime]("t", ColumnType.DateTime)
  private val otherStamp = NativeColumn[ZonedDateTime]("t2", ColumnType.DateTime)

  it should "subtract one datetime column from another" in {
    toSql(select(stamp - otherStamp).internalQuery, None) should matchSQL("SELECT t - t2")
  }

  it should "subtract a datetime literal from a column" in {
    val moment = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    toSql(select(stamp - moment).internalQuery, None) should matchSQL(
      "SELECT t - toDateTime('2020-01-01 00:00:00')"
    )
  }

  it should "keep the existing datetime-plus-number arithmetic" in {
    toSql(select(stamp + 1).internalQuery, None) should matchSQL("SELECT t + 1")
  }

  it should "refuse to add two datetimes" in {
    """select(stamp + otherStamp)""" shouldNot typeCheck
  }
}
