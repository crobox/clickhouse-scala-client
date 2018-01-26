package com.crobox.clickhouse.dsl.clickhouse

import java.util.UUID

import com.crobox.clickhouse.dsl.ClickhouseStatement
import com.crobox.clickhouse.partitioning.PartitionDateFormatter
import org.joda.time.DateTime

import scala.collection.immutable.Iterable

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

    override def apply(v: Boolean): String = v.toString

    override def unapply(v: String): Boolean = v.toBoolean
  }

  implicit object FloatQueryValue extends QueryValue[Float] {

    override def apply(v: Float): String = v.toString

    override def unapply(v: String): Float = v.toFloat
  }

  implicit object LongQueryValue extends QueryValue[Long] {

    override def apply(v: Long): String = v.toString

    override def unapply(v: String): Long = v.toLong
  }

  implicit object StringQueryValue extends QueryValue[String] {

    override def apply(v: String): String = quote(ClickhouseStatement.escape(v))

    //TODO: Unescape?
    override def unapply(v: String): String = unquote(v)
  }

  implicit object UUIDQueryValue extends QueryValue[UUID] {

    override def apply(v: UUID): String = quote(v.toString)

    override def unapply(v: String): UUID = UUID.fromString(unquote(v))
  }

  implicit object DateTimeQueryValue extends QueryValue[DateTime] {

    override def apply(v: DateTime): String = quote(PartitionDateFormatter.dateFormat(v))

    override def unapply(v: String): DateTime = DateTime.parse(unquote(v))
  }

  implicit def queryValueToSeq[V](ev: QueryValue[V]): QueryValue[Iterable[V]] =
    new IterableQueryValue(ev)

  class IterableQueryValue[V](ev: QueryValue[V]) extends QueryValue[Iterable[V]] {

    override def apply(value: Iterable[V]): String =
      s"""(${value.map(ev.apply).mkString(",")})"""

    override def unapply(queryRep: String): Iterable[V] =
      unquote(queryRep).split(",").map(ev.unapply).asInstanceOf[Iterable[V]]
  }

  private def unquote(v: String) =
    v.substring(1, v.length - 2)

  private def quote(v: String) =
    "'" + v + "'"

}

object QueryValueFormats extends QueryValueFormats
