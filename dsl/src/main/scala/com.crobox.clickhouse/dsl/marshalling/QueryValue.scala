package com.crobox.clickhouse.dsl.marshalling

import java.util.UUID

import com.crobox.clickhouse.dsl.ClickhouseStatement
import com.crobox.clickhouse.partitioning.PartitionDateFormatter
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, LocalDate}

import scala.annotation.implicitNotFound

/**
  * Parse a value into its Clickhouse SQL representation and vice - versa.
  *
  * @tparam V
  */
@implicitNotFound("No QueryVal for type ${V} in scope, import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._ or implement a QueryValue for ${V}")
trait QueryValue[V] {

  def apply(value: V): String

  def unapply(queryRep: String): V
}

trait QueryValueFormats {

  implicit object IntQueryValue extends QueryValue[Int] {

    override def apply(v: Int): String = v.toString

    override def unapply(v: String): Int = v.toInt
  }

  implicit object DoubleQueryValue extends QueryValue[Double] {

    override def apply(v: Double): String = v.toString

    override def unapply(v: String): Double = v.toDouble
  }

  implicit object ByteQueryValue extends QueryValue[Byte] {

    override def apply(v: Byte): String = v.toString

    override def unapply(v: String): Byte = v.toByte
  }

  implicit object BooleanQueryValue extends QueryValue[Boolean] {

    override def apply(v: Boolean): String = IntQueryValue.apply(if (v) 1 else 0)

    override def unapply(v: String): Boolean = IntQueryValue.unapply(v) == 1
  }

  implicit object FloatQueryValue extends QueryValue[Float] {

    override def apply(v: Float): String = v.toString

    override def unapply(v: String): Float = v.toFloat
  }

  implicit object LongQueryValue extends QueryValue[Long] {

    override def apply(v: Long): String = v.toString

    override def unapply(v: String): Long = v.toLong
  }

  implicit object BigDecimalQueryValue extends QueryValue[BigDecimal] {

    override def apply(v: BigDecimal): String = v.toString

    override def unapply(v: String): BigDecimal = BigDecimal(v)
  }

  implicit object BigIntQueryValue extends QueryValue[BigInt] {

    override def apply(v: BigInt): String = v.toString

    override def unapply(v: String): BigInt = BigInt(v)
  }

  implicit object StringQueryValue extends QueryValue[String] {

    override def apply(v: String): String = quote(ClickhouseStatement.escape(v))

    override def unapply(v: String): String = unquote(v)
  }

  implicit object UUIDQueryValue extends QueryValue[UUID] {

    override def apply(v: UUID): String = quote(v.toString)

    override def unapply(v: String): UUID = UUID.fromString(unquote(v))
  }

  implicit object DateTimeQueryValue extends QueryValue[DateTime] {
    private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    override def apply(v: DateTime): String = quote(formatter.print(v))

    override def unapply(v: String): DateTime = formatter.parseDateTime(unquote(v))
  }

  implicit object LocalDateQueryValue extends QueryValue[LocalDate] {

    override def apply(v: LocalDate): String = quote(PartitionDateFormatter.dateFormat(v))

    override def unapply(v: String): LocalDate = LocalDate.parse(unquote(v))
  }

  implicit def queryValueToSeq[V](ev: QueryValue[V]): QueryValue[scala.Iterable[V]] =
    new IterableQueryValue(ev)

  class IterableQueryValue[V](ev: QueryValue[V]) extends QueryValue[scala.Iterable[V]] {

    override def apply(value: scala.Iterable[V]): String =
      s"[${value.map(ev.apply).mkString(",")}]".toString

    override def unapply(queryRep: String): scala.Iterable[V] =
      unquote(queryRep).split(",").map(ev.unapply).asInstanceOf[scala.Iterable[V]]
  }

  private def unquote(v: String): String = v.substring(1, v.length - 2)

  private def quote(v: String): String = s"'$v'".toString

}

object QueryValueFormats extends QueryValueFormats
