package com.crobox.clickhouse.dsl.marshalling

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParseException}
import java.time.temporal.ChronoField
import java.time.{LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}
import spray.json._

import java.util.UUID
import scala.annotation.implicitNotFound

/**
 * Decodes one value of a ClickHouse result row into `A`.
 *
 * Deliberately lenient about how a number is spelled. ClickHouse serialises 64-bit integers as JSON strings or JSON
 * numbers depending on `output_format_json_quote_64bit_integers`, whose default flipped from 1 to 0 between 25.3 and
 * 25.8 -- which silently broke every hand-written reader that expected one or the other. A decoder that knows it is
 * looking at a `UInt64` does not need to care, which is why this layer makes pinning that setting unnecessary.
 */
@implicitNotFound(
  "No ColumnDecoder for ${A}. Import com.crobox.clickhouse.dsl.marshalling.ColumnDecoder._ or define one."
)
trait ColumnDecoder[A] {
  def decode(value: JsValue): A
}

object ColumnDecoder {

  def apply[A](implicit decoder: ColumnDecoder[A]): ColumnDecoder[A] = decoder

  private def instance[A](name: String)(f: PartialFunction[JsValue, A]): ColumnDecoder[A] =
    (value: JsValue) =>
      f.applyOrElse(
        value,
        (unexpected: JsValue) => throw ColumnDecodingException(name, unexpected)
      )

  /** A number, whether ClickHouse wrote it as a JSON number or quoted it as a string. */
  private def numeric[A](name: String)(fromBigDecimal: BigDecimal => A): ColumnDecoder[A] =
    instance(name) {
      case JsNumber(n) => fromBigDecimal(n)
      case JsString(s) =>
        try fromBigDecimal(BigDecimal(s))
        catch { case _: NumberFormatException => throw ColumnDecodingException(name, JsString(s)) }
    }

  implicit val byteDecoder: ColumnDecoder[Byte]             = numeric("Byte")(_.toByte)
  implicit val shortDecoder: ColumnDecoder[Short]           = numeric("Short")(_.toShort)
  implicit val intDecoder: ColumnDecoder[Int]               = numeric("Int")(_.toInt)
  implicit val longDecoder: ColumnDecoder[Long]             = numeric("Long")(_.toLong)
  implicit val bigIntDecoder: ColumnDecoder[BigInt]         = numeric("BigInt")(_.toBigInt)
  implicit val floatDecoder: ColumnDecoder[Float]           = numeric("Float")(_.toFloat)
  implicit val doubleDecoder: ColumnDecoder[Double]         = numeric("Double")(_.toDouble)
  implicit val bigDecimalDecoder: ColumnDecoder[BigDecimal] = numeric("BigDecimal")(identity)

  implicit val stringDecoder: ColumnDecoder[String] = instance("String") { case JsString(s) => s }

  /** ClickHouse has no Bool in older versions and renders it as 0/1; accept both that and a real JSON boolean. */
  implicit val booleanDecoder: ColumnDecoder[Boolean] = instance("Boolean") {
    case JsBoolean(b)      => b
    case JsNumber(n)       => n != 0
    case JsString("true")  => true
    case JsString("false") => false
    case JsString("1")     => true
    case JsString("0")     => false
  }

  implicit val uuidDecoder: ColumnDecoder[UUID] = instance("UUID") { case JsString(s) =>
    try UUID.fromString(s)
    catch { case _: IllegalArgumentException => throw ColumnDecodingException("UUID", JsString(s)) }
  }

  /** `Nullable(T)` arrives as `null`. */
  implicit def optionDecoder[A](implicit inner: ColumnDecoder[A]): ColumnDecoder[Option[A]] =
    (value: JsValue) =>
      value match {
        case JsNull => None
        case other  => Some(inner.decode(other))
      }

  implicit def seqDecoder[A](implicit inner: ColumnDecoder[A]): ColumnDecoder[Seq[A]] =
    instance("Array") { case JsArray(elements) => elements.map(inner.decode) }

  implicit def iterableDecoder[A](implicit inner: ColumnDecoder[A]): ColumnDecoder[Iterable[A]] =
    seqDecoder(inner).decode(_)

  /**
   * `Date` arrives as `2026-08-21`, `DateTime` as `2026-08-21 21:48:17` and `DateTime64(n)` with `n` fractional digits
   * -- a space rather than the ISO `T`, and no offset. Everything after the date is optional, because `n` runs from 0
   * to 9: `DateTime64(0)` carries no fractional part at all and `DateTime64(9)` carries nine digits.
   *
   * The absence of an offset means the server renders a `DateTime` in that column's own timezone, which the declared
   * type carries (`DateTime('Europe/Amsterdam')`) but the value does not; these read as UTC, which is right for a
   * default server and wrong for a column declared with an explicit zone. Reading the zone off the declared type needs
   * the decoder to see it, which this signature does not allow.
   */
  private val dateTimeParser: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd")
    .optionalStart()
    .appendLiteral(' ')
    .appendPattern("HH:mm:ss")
    .optionalStart()
    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
    .optionalEnd()
    .optionalEnd()
    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
    .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
    .toFormatter

  implicit val dateTimeDecoder: ColumnDecoder[ZonedDateTime] = instance("DateTime") { case JsString(s) =>
    try LocalDateTime.parse(s, dateTimeParser).atZone(ZoneOffset.UTC)
    catch { case _: DateTimeParseException => throw ColumnDecodingException("DateTime", JsString(s)) }
  }

  implicit val localDateDecoder: ColumnDecoder[LocalDate] = instance("Date") { case JsString(s) =>
    try LocalDate.parse(s, dateTimeParser)
    catch { case _: DateTimeParseException => throw ColumnDecodingException("Date", JsString(s)) }
  }

  /** Escape hatch for a column this layer has no instance for. */
  implicit val jsValueDecoder: ColumnDecoder[JsValue] = (value: JsValue) => value
}

case class ColumnDecodingException(expected: String, value: JsValue)
    extends RuntimeException(s"Expected $expected but got ${value.compactPrint}")
