package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

import scala.annotation.implicitNotFound




object DateTimeFunctions {
  @implicitNotFound("DateFunction can only be used on column of type LocalDate or DateTime")
  type DateOrDateTime[T] = Union[T, TableColumn[LocalDate] with TableColumn[DateTime]]

  abstract class DateTimeFunction[V <: AnyTableColumn : DateOrDateTime](val ddt: V)
    extends ExpressionColumn[V](ddt)

  case class Year[V: DateOrDateTime](d: V) extends DateTimeFunction[V](d)

  trait DateTimeFunctionsDsl {
    def toYear[T: DateOrDateTime](col: T): ExpressionColumn[_] = Year(col)
  }
}
