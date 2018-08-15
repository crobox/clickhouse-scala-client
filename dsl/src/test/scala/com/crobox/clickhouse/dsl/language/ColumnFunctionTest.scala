package com.crobox.clickhouse.dsl.language

import java.time.format.DateTimeFormatter

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.column._
import com.crobox.clickhouse.dsl.execution.ClickhouseQueryExecutor
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import org.joda.time.format.DateTimeFormat
import org.joda.time._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class ColumnFunctionTest extends ClickhouseClientSpec with TestSchemaClickhouseQuerySpec with ScalaFutures with ClickhouseTokenizerModule {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val clickhouseClient = clickClient
  implicit val db: Database = clickhouseClient.database

  val ex = ClickhouseQueryExecutor.default(clickhouseClient)

  "ColumnFunction tokenization and execution" should "succeed for ArithmeticFunctions" in {
    def leftRightFunctions[L,R,O](implicit ev: AritRetType[L,R,O]): Seq[(String,(NumericCol[L], NumericCol[R]) => ArithmeticFunctionOp[O])] = Seq(
      ("5", plus[L, R, O]),
      ("1", minus[L, R, O]),
      ("6", multiply[L, R, O]),
      ("1.5", divide[L, R, O]),
      ("1", intDiv[L, R, O]),
      ("1", intDivOrZero[L, R, O]),
      ("1", modulo[L, R, O]),
      ("1", gcd[L, R, O]),
      ("6", lcm[L, R, O])
    )

    leftRightFunctions[Int,Int,Int].foreach(fun =>{
      r(fun._2.apply(3,2)) shouldBe fun._1
    })

    r(negate(3)) shouldBe "-3"
    r(abs(-3)) shouldBe "3"
  }

  it should "succeed for ArrayFunctions" in {
    Seq(
      r(emptyArrayUInt8),
      r(emptyArrayUInt16),
      r(emptyArrayUInt32),
      r(emptyArrayUInt64),
      r(emptyArrayInt8),
      r(emptyArrayInt16),
      r(emptyArrayInt32),
      r(emptyArrayInt64),
      r(emptyArrayFloat32),
      r(emptyArrayFloat64),
      r(emptyArrayDate),
      r(emptyArrayDateTime),
      r(emptyArrayString)
    ).foreach(result => result shouldBe "[]")

    r(emptyArrayToSingle(emptyArrayUInt8)) shouldBe "[0]"
    r(range(3)) shouldBe "[0,1,2]"

    r(arrayConcat(range(2),Seq(2L,3L))) shouldBe "[0,1,2,3]"

    val arbArray = range(3)

    r(indexOf(arbArray,2L)) shouldBe "3"
    r(countEqual(arbArray,2L)) shouldBe "1"
    r(arrayEnumerate(arbArray)) shouldBe "[1,2,3]"
    r(arrayEnumerateUniq(arbArray)) shouldBe "[1,1,1]"
    r(arrayPopBack(arbArray)) shouldBe "[0,1]"
    r(arrayPopFront(arbArray)) shouldBe "[1,2]"
    r(arrayPushBack(arbArray,4L)) shouldBe "[0,1,2,4]"
    r(arrayPushFront(arbArray,4L)) shouldBe "[4,0,1,2]"
    r(arraySlice(arbArray,2,1)) shouldBe "[1]"
    r(arrayUniq(arbArray)) shouldBe "3"
    r(arrayJoin(arbArray)) shouldBe "0\n1\n2"
  }

  it should "succeed for BitFunctions" in {
    r(bitAnd(0,1)) shouldBe "0"
    r(bitOr(0,1)) shouldBe "1"
    r(bitXor(0,1)) shouldBe "1"
    r(bitNot(64)) shouldBe (255 - 64).toString
    r(bitShiftLeft(2,2)) shouldBe "8"
    r(bitShiftRight(8,1)) shouldBe "4"

  }

  it should "succeed for ComparisonFunctions" in {
    val someNum = const(10L)

    r(someNum <> 3) shouldBe "1"
    r(someNum > 3) shouldBe "1"
    r(someNum < 3) shouldBe "0"
    r(someNum >= 3) shouldBe "1"
    r(someNum <= 3) shouldBe "0"
    r(someNum isEq 3) shouldBe "0"
    r(notEquals(1,2)) shouldBe "1"
    r(_equals(2L,2)) shouldBe "1"
    r(less(1.0,200)) shouldBe "1"
    r(greater(1L,2L)) shouldBe "0"
    r(lessOrEquals(1,2)) shouldBe "1"
    r(greaterOrEquals(1,2)) shouldBe "0"
  }

  implicit class DDTStringify(ddt: DateTime) {
    def printAsDate: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(ddt)

    def printAsDateTime: String = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(ddt)

    def printAsYYYYMM: String = DateTimeFormat.forPattern("yyyyMM").print(ddt)

    def toStartOfQuarter = {
      val remainder = (ddt.getMonthOfYear - 1) % 3

      ddt.withDayOfMonth(1).minusMonths(remainder)
    }

    def toStartOfMin(min: Int) = {
      val remainder = ddt.getMinuteOfHour % min

      ddt
        .withSecondOfMinute(0)
        .withMillisOfSecond(0)
        .minusMinutes(remainder)
    }

    def toStartOfHr = {
      ddt
        .withMinuteOfHour(0)
        .withSecondOfMinute(0)
        .withMillisOfSecond(0)
    }
  }

  it should "succeed for DateTimeFunctions" in {
    def now = new DateTime().withZone(DateTimeZone.UTC)
    val epoch = new DateTime(0).withZone(DateTimeZone.UTC)

    r(toYear(now)) shouldBe now.getYear.toString
    r(toYYYYMM(now)) shouldBe now.printAsYYYYMM
    r(toMonth(now)) shouldBe now.getMonthOfYear.toString
    r(toDayOfMonth(now)) shouldBe now.getDayOfMonth.toString
    r(toDayOfWeek(now)) shouldBe now.getDayOfWeek.toString
    r(toHour(now)) shouldBe now.getHourOfDay.toString
    r(toMinute(now)) shouldBe now.getMinuteOfHour.toString
    r(toSecond(now)) shouldBe now.getSecondOfMinute.toString
    r(toMonday(now)) shouldBe now.withDayOfWeek(1).printAsDate
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
    r(toRelativeMonthNum(now)) shouldBe ((now.getYear * 12) + now.getMonthOfYear).toString
    r(toRelativeWeekNum(now)) shouldBe (Weeks.weeksBetween(epoch, now).getWeeks + 1).toString
    r(toRelativeDayNum(now)) shouldBe Days.daysBetween(epoch, now).getDays.toString
    r(toRelativeHourNum(now)) shouldBe Hours.hoursBetween(epoch, now).getHours.toString
    r(toRelativeMinuteNum(now)) shouldBe Minutes.minutesBetween(epoch, now).getMinutes.toString
    r(toRelativeSecondNum(now)) shouldBe Seconds.secondsBetween(epoch, now).getSeconds.toString
    r(chNow()) shouldBe now.printAsDateTime
    r(chYesterday()) shouldBe now.minusDays(1).printAsDate
    r(chToday()) shouldBe now.withTimeAtStartOfDay().printAsDate
    r(timeSlot(now)) shouldBe now.toStartOfMin(30).printAsDateTime
    r(timeSlots(now,toUInt32(1800))) shouldBe s"['${now.toStartOfMin(30).printAsDateTime}','${now.plusMinutes(30).toStartOfMin(30).printAsDateTime}']"
  }

  it should "succeed for DictionaryFunctions" in {}
  it should "succeed for EncodingFunctions" in {}
  it should "succeed for HashFunctions" in {}
  it should "succeed for HigherOrderFunctions" in {}
  it should "succeed for IPFunctions" in {}
  it should "succeed for JsonFunctions" in {}
  it should "succeed for LogicalFunctions" in {}
  it should "succeed for MathFunctions" in {}
  it should "succeed for MiscFunctions" in {}
  it should "succeed for RandomFunctions" in {}
  it should "succeed for RoundingFunctions" in {}
  it should "succeed for SplitMergeFunctions" in {}
  it should "succeed for StringFunctions" in {}
  it should "succeed for StringSearchFunctions" in {}
  it should "succeed for TypeCastFunctions" in {}
  it should "succeed for URLFunctions" in {}

  private def r(query: AnyTableColumn): String = {
    runSql(select(query)).futureValue.trim
  }

  private def runSql(query: OperationalQuery): Future[String] = {
    clickhouseClient.query(toSql(query.internalQuery,None))
  }
}
