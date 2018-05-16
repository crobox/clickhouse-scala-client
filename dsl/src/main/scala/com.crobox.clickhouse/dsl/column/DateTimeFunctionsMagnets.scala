package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

import scala.annotation.implicitNotFound


object DateTimeFunctionsMagnets {

  sealed trait DDTimeMagnet {
    type ColumnType
    val column: TableColumn[ColumnType]
  }
  object DDTimeMagnet {
    implicit def fromDate[T <: TableColumn[LocalDate]](s: T) =
      new DDTimeMagnet {
        override type ColumnType = LocalDate
        override val column = s
      }

    implicit def fromDateTime[T <: TableColumn[DateTime]](s: T) =
      new DDTimeMagnet {
        override type ColumnType = DateTime
        override val column = s
      }
  }

  abstract class DateTimeFunction[V](val ddt: DDTimeMagnet) extends ExpressionColumn[V](ddt.column)

  case class Year(d: DDTimeMagnet) extends DateTimeFunction[Int](d)

  trait DateTimeFunctionsDsl {
    def toYear(col: DDTimeMagnet) = Year(col)
  }
}
