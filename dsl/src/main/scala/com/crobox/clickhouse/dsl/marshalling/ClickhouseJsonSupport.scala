package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.time.IntervalStart
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId, ZoneOffset, ZonedDateTime}
import spray.json.{deserializationError, JsNumber, JsString, JsValue, JsonFormat, _}

import scala.util.Try
import scala.util.matching.Regex

trait ClickhouseJsonSupport {

  /**
   * Adds support for the date formats clickhouse returns for grouped intervals
   */
  implicit object ClickhouseIntervalStartFormat extends JsonFormat[IntervalStart] {

    override def write(obj: IntervalStart): JsValue = JsNumber(obj.toInstant.toEpochMilli)

    val month: Regex = """(\d+)_(.*)""".r

    // Lazy: a zone id can contain an underscore (America/New_York), a date cannot, so split on the first.
    val date: Regex                  = """(.+?)_(.*)""".r
    val nanoTimestamp: Regex         = """^(\d{16})$""".r
    val msTimestamp: Regex           = """^(\d{13})$""".r
    val timestamp: Regex             = """^(\d{10})$""".r
    val RelativeMonthsSinceUnixStart = 23641

    val UnixStart: LocalDate             = LocalDate.of(1970, 1, 1)
    val formatter: DateTimeFormatter     = DateTimeFormatter.ISO_LOCAL_DATE
    val readFormatter: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    /*
     * It read the dates back as UTC, but it does contain the corresponding milliseconds relative to the timezone used in the initial request
     *
     * */
    override def read(json: JsValue): IntervalStart =
      json match {
        case JsString(value) =>
          value match {
            case month(relativeMonth, timezoneId) =>
              UnixStart
                .atStartOfDay(ZoneId.of(timezoneId))
                .plusMonths((relativeMonth.toInt - RelativeMonthsSinceUnixStart).toLong)
                .withZoneSameInstant(ZoneOffset.UTC)
            case date(dateOnly, timezoneId) =>
              // should handle quarter and year grouping as it returns a date
              LocalDate
                .parse(dateOnly, formatter)
                .atStartOfDay(ZoneId.of(timezoneId))
                .withZoneSameInstant(ZoneOffset.UTC)
            case nanoTimestamp(millis) => atUtc(millis.toLong / 1000)
            case msTimestamp(millis)   => atUtc(millis.toLong)
            case timestamp(secs)       => atUtc(secs.toLong * 1000)
            case _                     =>
              // sometimes clickhouse mistakenly returns a long / int value as JsString. Therefor, first try to
              // parse it as a long...
              val dateTime = Try(atUtc(value.toLong)).toOption

              // continue with parsing using the formatter
              dateTime.getOrElse {
                try
                  LocalDate.parse(value, formatter).atStartOfDay(ZoneId.systemDefault())
                catch {
                  case _: java.time.format.DateTimeParseException =>
                    error(s"Couldn't parse $value into valid date time")
                  case _: UnsupportedOperationException =>
                    error("Unsupported operation, programmatic misconfiguration?")
                }
              }
          }
        case JsNumber(millis) => atUtc(millis.longValue)
        case _                => throw DeserializationException(s"Unknown date format read from clickhouse for $json")
      }

    private def atUtc(millis: Long): ZonedDateTime = Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC)

    def error(v: Any): ZonedDateTime = {
      val example = readFormatter.format(Instant.EPOCH.atZone(ZoneOffset.UTC))
      deserializationError(
        f"'$v' is not a valid date value. Dates must be in compact ISO-8601 format, e.g. '$example'"
      )
    }
  }

}
object ClickhouseJsonSupport extends DefaultJsonProtocol with ClickhouseJsonSupport
