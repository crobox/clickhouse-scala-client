package com.crobox.clickhouse.dsl.marshalling

import java.util.UUID

import com.crobox.clickhouse.dsl.Limit
import com.crobox.clickhouse.time.TimeUnit._
import com.crobox.clickhouse.time.{Duration, IntervalStart, MultiDuration, MultiInterval, MultiTimeUnit, SimpleDuration, SimpleTimeUnit, TimeUnit}
import org.joda.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, Interval}
import spray.json.{DefaultJsonProtocol, JsNumber, JsObject, JsString, JsValue, JsonFormat, RootJsonFormat, deserializationError, _}

import scala.collection.GenTraversableOnce

object JsonProtocol extends DefaultJsonProtocol {

  /**
   * Adds support for org.joda.time.DateTime format specific to clickhouse
   */
  implicit object ClickhouseIntervalStartFormat extends JsonFormat[IntervalStart] {

    override def write(obj: IntervalStart): JsValue = JsNumber(obj.getMillis)

    val month                        = """(\d+)_(.*)""".r
    val date                         = """(.+)_(.*)""".r
    val timestamp                    = """^(\d{13})$""".r
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
            case timestamp(millis) => new DateTime(millis.toLong, DateTimeZone.UTC)
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

  /**
   * Adds support for UUID format in spray-json
   */
  implicit object UUIDFormat extends JsonFormat[UUID] {

    def write(obj: UUID): JsValue = {
      require(obj ne null)
      JsString(obj.toString)
    }

    def read(json: JsValue): UUID = json match {
      case JsString(s) =>
        try {
          UUID.fromString(s)
        } catch {
          case t: Throwable => deserializationError(s"Invalid UUID '$s' found", t)
        }
      case _ => deserializationError("String expected")
    }
  }

  implicit object TimeUnitFormat extends JsonFormat[TimeUnit] {

    override def write(obj: TimeUnit): JsValue = JsString(obj.labels.head)

    override def read(json: JsValue): TimeUnit = json match {
      case JsString(unit) => TimeUnit.lookup(unit)
      case _              => deserializationError("String expected")
    }
  }

  implicit object DurationFormat extends JsonFormat[Duration] {
    val allowedUnits = Seq(Hour, Day, Week, Month, Quarter, Year, Total)

    override def read(json: JsValue): Duration =
      json match {
        case JsString(shortcut) =>
          Duration.parse(shortcut)
        case obj: JsObject =>
          obj.getFields("value", "unit") match {
            case Seq(JsNumber(value), unit) =>
              buildDuration(unit.convertTo[TimeUnit], value.intValue())
            case Seq(unit) =>
              buildDuration(unit.convertTo[TimeUnit], 1)
          }
        case _ => deserializationError("String or Object expected")
      }

    override def write(obj: Duration): JsValue = obj match {
      case SimpleDuration(unit) =>
        JsObject(
          "unit" -> unit.asInstanceOf[TimeUnit].toJson
        )
      case MultiDuration(value, unit) =>
        JsObject(
          "value" -> JsNumber(value),
          "unit"  -> unit.asInstanceOf[TimeUnit].toJson
        )
    }

    private def buildDuration(unit: TimeUnit, parsedValue: Int) =
      unit match {
        case unit: SimpleTimeUnit =>
          SimpleDuration(unit)
        case unit: MultiTimeUnit =>
          MultiDuration(parsedValue, unit)
      }
  }

  implicit object MultiIntervalFormat extends JsonFormat[MultiInterval] {

    override def read(json: JsValue): MultiInterval =
      json.asJsObject.getFields("start", "end", "duration") match {
        case Seq(start, end, duration) =>
          MultiInterval(start.convertTo[DateTime], end.convertTo[DateTime], duration.convertTo[Duration])
      }

    override def write(obj: MultiInterval): JsValue = JsObject(
      "start"    -> obj.getStart.toJson,
      "end"      -> obj.getEnd.toJson,
      "duration" -> obj.duration.toJson
    )
  }

  implicit object IntervalJsonProtocol extends RootJsonFormat[Interval] {

    override def write(obj: Interval): JsValue =
      JsObject(
        "start" -> obj.getStart.toJson,
        "end"   -> obj.getEnd.toJson
      )

    override def read(json: JsValue): Interval =
      json.asJsObject.getFields("start", "end") match {
        case Seq(start, end) => new Interval(start.convertTo[DateTime], end.convertTo[DateTime])
        case _ =>
          throw new IllegalArgumentException(s"Wrong input $json for interval. Expected start and end as dates.")
      }
  }

  implicit def optional(member: (String, Option[JsValue])): GenTraversableOnce[(String, JsValue)] =
    member._2.map((member._1, _)).toSeq

  // there should be more elegant solution to this
  // parameters could be "size":"30" instead of "size":30
  def convert[V <: Long](el: JsValue): Long =
    el match {
      case JsString(str) => str.toLong
      case JsNumber(num) => num.toLong
      case _             => throw DeserializationException(s"Cannot convert $el to long")
    }

  def optionalBoolean(value: Option[JsValue]): Boolean =
    value.exists(_ match {
      case JsString("") | JsString("true") | JsString("on") | JsString("yes") => true
      case JsString("false") | JsString("no") | JsString("off")               => false
      case JsBoolean(vl)                                                      => vl
      case _                                                                  => throw new IllegalArgumentException("Could not parse expected boolean value")
    })

  implicit object LimitProtocol extends JsonFormat[Limit] {
    val format = jsonFormat2(Limit.apply)

    override def write(obj: Limit): JsValue = format.write(obj)

    override def read(json: JsValue): Limit = {
      val jsObject = json.asJsObject
      val getter   = jsObject.fields.get _
      jsObject.getFields("size", "offset") match {
        case Seq(size, offset) => Limit(convert(size), convert(offset))
        case Seq(_) =>
          getter("size")
            .map(size => Limit(convert[Long](size)))
            .getOrElse(Limit(offset = convert(getter("offset").get)))
        case _ => Limit()
      }
    }
  }

}
