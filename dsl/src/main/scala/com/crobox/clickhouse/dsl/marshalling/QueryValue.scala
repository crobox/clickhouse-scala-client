package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.dsl.ClickhouseStatement
import com.crobox.clickhouse.partitioning.PartitionDateFormatter
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, LocalDate}
import scala.language.implicitConversions

import java.util.UUID
import scala.annotation.implicitNotFound

/**
 * Render a value as its Clickhouse SQL literal representation.
 *
 * This used to also declare `unapply`, parsing a literal back into `V`. That half had no caller anywhere in the
 * codebase and was broken -- its `unquote` dropped two trailing characters where `quote` had added one, so `'abc'` came
 * back as `"ab"`. Decoding query *results* is a separate concern with a separate shape (results are JSON, not SQL
 * literals), so the reverse direction is gone rather than fixed.
 *
 * @tparam V
 */
@implicitNotFound(
  "No QueryVal for type ${V} in scope, import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._ or implement a QueryValue for ${V}"
)
trait QueryValue[V] {

  def apply(value: V): String
}

trait QueryValueFormats {

  implicit object IntQueryValue extends QueryValue[Int] {
    override def apply(v: Int): String = v.toString
  }

  implicit object DoubleQueryValue extends QueryValue[Double] {
    override def apply(v: Double): String = v.toString
  }

  implicit object ByteQueryValue extends QueryValue[Byte] {
    override def apply(v: Byte): String = v.toString
  }

  implicit object BooleanQueryValue extends QueryValue[Boolean] {
    override def apply(v: Boolean): String = IntQueryValue.apply(if (v) 1 else 0)
  }

  implicit object FloatQueryValue extends QueryValue[Float] {
    override def apply(v: Float): String = v.toString
  }

  implicit object LongQueryValue extends QueryValue[Long] {
    override def apply(v: Long): String = v.toString
  }

  implicit object BigDecimalQueryValue extends QueryValue[BigDecimal] {
    override def apply(v: BigDecimal): String = v.toString
  }

  implicit object BigIntQueryValue extends QueryValue[BigInt] {
    override def apply(v: BigInt): String = v.toString
  }

  implicit object StringQueryValue extends QueryValue[String] {
    override def apply(v: String): String = quote(ClickhouseStatement.escape(v))
  }

  implicit object UUIDQueryValue extends QueryValue[UUID] {
    override def apply(v: UUID): String = quote(v.toString)
  }

  implicit object DateTimeQueryValue extends QueryValue[DateTime] {
    private val formatter                   = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    override def apply(v: DateTime): String = quote(formatter.print(v))
  }

  implicit object LocalDateQueryValue extends QueryValue[LocalDate] {
    override def apply(v: LocalDate): String = quote(PartitionDateFormatter.dateFormat(v))
  }

  implicit def queryValueToSeq[V](ev: QueryValue[V]): QueryValue[scala.Iterable[V]] =
    new IterableQueryValue(ev)

  class IterableQueryValue[V](ev: QueryValue[V]) extends QueryValue[scala.Iterable[V]] {
    override def apply(value: scala.Iterable[V]): String = s"[${value.map(ev.apply).mkString(", ")}]"
  }

  private def quote(v: String): String = s"'$v'"
}

object QueryValueFormats extends QueryValueFormats
