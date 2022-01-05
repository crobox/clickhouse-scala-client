package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{ClickhouseClientSpec, TestSchemaClickhouseQuerySpec}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

trait ColumnFunctionTest
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures
    with ClickhouseTokenizerModule {
  implicit val clickhouseClient = clickClient

  protected def r(query: Column): String =
    runSql(select(query)).futureValue.trim

  protected def runSql(query: OperationalQuery): Future[String] =
    clickhouseClient.query(toSql(query.internalQuery, None))

  implicit class DDTStringify(ddt: DateTime) {
    def printAsDate: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(ddt)

    def printAsDateTime: String = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(ddt)

    def printAsYYYYMM: String = DateTimeFormat.forPattern("yyyyMM").print(ddt)

    def toStartOfQuarter: DateTime = {
      val remainder = (ddt.getMonthOfYear - 1) % 3

      ddt.withDayOfMonth(1).minusMonths(remainder)
    }

    def toStartOfMin(min: Int): DateTime = {
      val remainder = ddt.getMinuteOfHour % min

      ddt
        .withSecondOfMinute(0)
        .withMillisOfSecond(0)
        .minusMinutes(remainder)
    }

    def toStartOfHr: DateTime =
      ddt
        .withMinuteOfHour(0)
        .withSecondOfMinute(0)
        .withMillisOfSecond(0)
  }
}
