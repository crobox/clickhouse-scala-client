package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.time.IntervalStart
import org.joda.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}
import spray.json.{JsNumber, JsString, JsValue, JsonFormat, deserializationError, _}

trait ClickhouseJsonSupport {

  /**
   * Adds support for org.joda.time.DateTime format specific to clickhouse
   */
  implicit object ClickhouseIntervalStartFormat extends JsonFormat[IntervalStart] {

    override def write(obj: IntervalStart): JsValue = JsNumber(obj.getMillis)

    val month                        = """(\d+)_(.*)""".r
    val date                         = """(.+)_(.*)""".r
    val msTimestamp                  = """^(\d{13})$""".r
    val timestamp                    = """^(\d{10})$""".r
    val RelativeMonthsSinceUnixStart = 23641

    val UnixStartTimeWithoutTimeZone            = "1970-01-01T00:00:00.000"
    val formatter: DateTimeFormatter            = ISODateTimeFormat.date()
    private val isoFormatter: DateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis

    val readFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
      .append(isoFormatter.getPrinter,
              Array(isoFormatter.getParser, ISODateTimeFormat.date().withZone(DateTimeZone.UTC).getParser))
      .toFormatter
      .withOffsetParsed()

    /*
     * It read the dates back as UTC, but it does contain the corresponding milliseconds relative to the timezone used in the initial request
     *
     * */
    override def read(json: JsValue): IntervalStart =
      json match {
        case JsString(value) =>
          value match {
            case month(relativeMonth, timezoneId) =>
              new DateTime(UnixStartTimeWithoutTimeZone)
                .withZoneRetainFields(DateTimeZone.forID(timezoneId))
                .plusMonths(relativeMonth.toInt - RelativeMonthsSinceUnixStart)
                .withZone(DateTimeZone.UTC)
            case date(dateOnly, timezoneId) =>
              //should handle quarter and year grouping as it returns a date
              formatter
                .parseDateTime(dateOnly)
                .withZoneRetainFields(DateTimeZone.forID(timezoneId))
                .withZone(DateTimeZone.UTC)
            case msTimestamp(millis) => new DateTime(millis.toLong, DateTimeZone.UTC)
            case timestamp(secs) => new DateTime(secs.toLong * 1000, DateTimeZone.UTC)
            case _ =>
              try {
                formatter.parseDateTime(value)
              } catch {
                case _: IllegalArgumentException      => error(s"Couldn't parse $value into valid date time")
                case _: UnsupportedOperationException => error("Unsupported operation, programmatic misconfiguration?")
              }
          }
        case JsNumber(millis) => new DateTime(millis.longValue(), DateTimeZone.UTC)
        case _                => throw DeserializationException(s"Unknown date format read from clickhouse for $json")
      }

    def error(v: Any): DateTime = {
      val example = readFormatter.print(0)
      deserializationError(
        f"'$v' is not a valid date value. Dates must be in compact ISO-8601 format, e.g. '$example'"
      )
    }
  }

}
object ClickhouseJsonSupport extends DefaultJsonProtocol with ClickhouseJsonSupport
