package com.crobox.clickhouse.dsl.language
import com.crobox.clickhouse.dsl._
import org.joda.time._

class DateTimeFunctionTest extends ColumnFunctionTest {
  "Tokenization" should "succeed for DateTimeFunctions" in {
    val now = new DateTime().withZone(DateTimeZone.UTC)
    val epoch = new DateTime(0).withZone(DateTimeZone.UTC)

    def dynNow = new DateTime().withZone(DateTimeZone.UTC)

    r(toYear(now)) shouldBe now.getYear.toString
    r(toYYYYMM(now)) shouldBe now.printAsYYYYMM
    r(toMonth(now)) shouldBe now.getMonthOfYear.toString
    r(toDayOfMonth(now)) shouldBe now.getDayOfMonth.toString
    r(toDayOfWeek(now)) shouldBe now.getDayOfWeek.toString
    r(toHour(now)) shouldBe now.getHourOfDay.toString
    r(toMinute(now)) shouldBe now.getMinuteOfHour.toString
    r(toSecond(now)) shouldBe now.getSecondOfMinute.toString
    r(toMonday(now)) shouldBe now.withDayOfWeek(1).printAsDate
    r(addSeconds(now,2)) shouldBe now.plusSeconds(2).printAsDateTime
    r(addMinutes(now,2)) shouldBe now.plusMinutes(2).printAsDateTime
    r(addHours(now,2)) shouldBe now.plusHours(2).printAsDateTime
    r(addDays(now,2)) shouldBe now.plusDays(2).printAsDateTime
    r(addWeeks(now,2)) shouldBe now.plusWeeks(2).printAsDateTime
    r(addMonths(now,2)) shouldBe now.plusMonths(2).printAsDateTime
    r(addYears(now,2)) shouldBe now.plusYears(2).printAsDateTime
    r(toStartOfMonth(now)) shouldBe now.withDayOfMonth(1).printAsDate
    r(toStartOfQuarter(now)) shouldBe now.toStartOfQuarter.printAsDate
    r(toStartOfYear(now)) shouldBe now.withDayOfYear(1).printAsDate
    r(toStartOfMinute(now)) shouldBe now.toStartOfMin(1).printAsDateTime
    r(toStartOfFiveMinute(now)) shouldBe now.toStartOfMin(5).printAsDateTime
    r(toStartOfFifteenMinutes(now)) shouldBe now.toStartOfMin(15).printAsDateTime
    r(toStartOfHour(now)) shouldBe now.toStartOfHr.printAsDateTime
    r(toStartOfDay(now)) shouldBe now.withTimeAtStartOfDay().printAsDateTime
    r(toTime(now)).substring(11) shouldBe now.printAsDateTime.substring(11)
    r(toRelativeYearNum(now)) shouldBe now.getYear.toString
    r(toRelativeQuarterNum(now)) shouldBe ((now.getYear * 4) + (now.getMonthOfYear - 1) / 3).toString
    r(toRelativeMonthNum(now)) shouldBe ((now.getYear * 12) + now.getMonthOfYear).toString
    r(toRelativeWeekNum(now)) should (equal(Weeks.weeksBetween(epoch, now).getWeeks.toString) or equal((Weeks.weeksBetween(epoch, now).getWeeks + 1).toString))
    r(toRelativeDayNum(now)) shouldBe Days.daysBetween(epoch, now).getDays.toString
    r(toRelativeHourNum(now)) shouldBe Hours.hoursBetween(epoch, now).getHours.toString
    r(toRelativeMinuteNum(now)) shouldBe Minutes.minutesBetween(epoch, now).getMinutes.toString
    r(toRelativeSecondNum(now)) shouldBe Seconds.secondsBetween(epoch, now).getSeconds.toString
    r(chNow()) should (equal(dynNow.printAsDateTime) or equal(dynNow.minusSeconds(1).printAsDateTime))
    r(chYesterday()) shouldBe dynNow.minusDays(1).printAsDate
    r(chToday()) shouldBe dynNow.withTimeAtStartOfDay().printAsDate
    r(timeSlot(now)) shouldBe now.toStartOfMin(30).printAsDateTime
    r(timeSlots(now,toUInt32(1800))) shouldBe s"['${now.toStartOfMin(30).printAsDateTime}','${now.plusMinutes(30).toStartOfMin(30).printAsDateTime}']"
  }
}
