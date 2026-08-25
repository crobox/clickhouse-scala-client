package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.TestUtils.DDTStringify
import com.crobox.clickhouse.dsl._
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}

class DateTimeFunctionsIT extends DslITSpec {
  it should "succeed for DateTimeFunctions" in {
    val now         = ZonedDateTime.now(ZoneOffset.UTC)
    val epoch       = java.time.Instant.EPOCH.atZone(ZoneOffset.UTC)
    val August_8_19 = ZonedDateTime.now(ZoneOffset.UTC).withYear(2019).withMonth(8).withDayOfMonth(8)

    def dynNow = ZonedDateTime.now(ZoneOffset.UTC)

    r(toYear(now)) shouldBe now.getYear.toString
    r(toYYYYMM(now)) shouldBe now.printAsYYYYMM
    r(toMonth(now)) shouldBe now.getMonthValue.toString
    r(toDayOfMonth(now)) shouldBe now.getDayOfMonth.toString
    r(toDayOfWeek(now)) shouldBe now.getDayOfWeek.getValue.toString
    r(toHour(now)) shouldBe now.getHour.toString
    r(toMinute(now)) shouldBe now.getMinute.toString
    r(toSecond(now)) shouldBe now.getSecond.toString
    r(toMonday(now)) shouldBe now
      .`with`(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY))
      .printAsDate
    r(addSeconds(now, 2)) shouldBe now.plusSeconds(2).printAsDateTime
    r(addMinutes(now, 2)) shouldBe now.plusMinutes(2).printAsDateTime
    r(addHours(now, 2)) shouldBe now.plusHours(2).printAsDateTime
    r(addDays(now, 2)) shouldBe now.plusDays(2).printAsDateTime
    r(addWeeks(now, 2)) shouldBe now.plusWeeks(2).printAsDateTime
    r(addMonths(now, 2)) shouldBe now.plusMonths(2).printAsDateTime
    r(addYears(now, 2)) shouldBe now.plusYears(2).printAsDateTime
    r(toStartOfMonth(now)) shouldBe now.withDayOfMonth(1).printAsDate
    r(toStartOfQuarter(now)) shouldBe now.toStartOfQuarter.printAsDate
    r(toStartOfYear(now)) shouldBe now.withDayOfYear(1).printAsDate
    r(toStartOfMinute(now)) shouldBe now.toStartOfMin(1).printAsDateTime
    r(toStartOfFiveMinute(now)) shouldBe now.toStartOfMin(5).printAsDateTime
    r(toStartOfFifteenMinutes(now)) shouldBe now.toStartOfMin(15).printAsDateTime
    r(toStartOfHour(now)) shouldBe now.toStartOfHr.printAsDateTime
    r(toStartOfDay(now)) shouldBe now.toLocalDate.atStartOfDay(ZoneOffset.UTC).printAsDateTime
    r(toTime(now)).substring(11) shouldBe now.printAsDateTime.substring(11)
    r(toRelativeYearNum(now)) shouldBe now.getYear.toString
    r(toRelativeQuarterNum(now)) shouldBe ((now.getYear * 4) + (now.getMonthValue - 1) / 3).toString
    r(toRelativeMonthNum(now)) shouldBe ((now.getYear * 12) + now.getMonthValue).toString
    r(toRelativeWeekNum(now)) should (equal(ChronoUnit.WEEKS.between(epoch, now).toString) or equal(
      (ChronoUnit.WEEKS.between(epoch, now) + 1).toString
    ))
    r(toRelativeDayNum(now)) shouldBe ChronoUnit.DAYS.between(epoch, now).toString
    r(toRelativeHourNum(now)) shouldBe ChronoUnit.HOURS.between(epoch, now).toString
    r(toRelativeMinuteNum(now)) shouldBe ChronoUnit.MINUTES.between(epoch, now).toString
    r(toRelativeSecondNum(now)) shouldBe ChronoUnit.SECONDS.between(epoch, now).toString
    r(chNow()) should (equal(dynNow.printAsDateTime) or equal(dynNow.minusSeconds(1).printAsDateTime))
    r(chYesterday()) shouldBe dynNow.minusDays(1).printAsDate
    r(chToday()) shouldBe dynNow.toLocalDate.atStartOfDay(ZoneOffset.UTC).printAsDate
    r(timeSlot(now)) shouldBe now.toStartOfMin(30).printAsDateTime
    r(
      timeSlots(now, toUInt32(1800))
    ) shouldBe s"['${now.toStartOfMin(30).printAsDateTime}','${now.plusMinutes(30).toStartOfMin(30).printAsDateTime}']"
    r(toISOWeek(August_8_19)) shouldBe "32"
    r(toISOYear(August_8_19)) shouldBe "2019"
    r(toWeek(August_8_19)) shouldBe "31"
    r(toWeek(August_8_19, mode = 0)) shouldBe "31"
    r(toWeek(August_8_19, mode = 1)) shouldBe "32"
    r(toWeek(August_8_19, mode = 2)) shouldBe "31"
    r(toWeek(August_8_19, mode = 3)) shouldBe "32"
    r(toWeek(August_8_19, mode = 4)) shouldBe "32"
    r(toWeek(August_8_19, mode = 5)) shouldBe "31"
    r(toWeek(August_8_19, mode = 6)) shouldBe "32"
    r(toWeek(August_8_19, mode = 7)) shouldBe "31"
    r(toWeek(August_8_19, mode = 8)) shouldBe "32"
    r(toWeek(August_8_19, mode = 9)) shouldBe "32"
  }
}
