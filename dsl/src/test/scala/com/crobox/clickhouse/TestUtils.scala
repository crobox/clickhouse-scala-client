package com.crobox.clickhouse

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object TestUtils {

  implicit class DDTStringify(ddt: ZonedDateTime) {
    def printAsDate: String = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(ddt)

    def printAsDateTime: String = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(ddt)

    def printAsYYYYMM: String = DateTimeFormatter.ofPattern("yyyyMM").format(ddt)

    def toStartOfQuarter: ZonedDateTime = {
      val remainder = (ddt.getMonthValue - 1) % 3

      ddt.withDayOfMonth(1).minusMonths(remainder.toLong)
    }

    def toStartOfMin(min: Int): ZonedDateTime = {
      val remainder = ddt.getMinute % min

      ddt.truncatedTo(ChronoUnit.MINUTES).minusMinutes(remainder.toLong)
    }

    def toStartOfHr: ZonedDateTime = ddt.truncatedTo(ChronoUnit.HOURS)
  }
}
