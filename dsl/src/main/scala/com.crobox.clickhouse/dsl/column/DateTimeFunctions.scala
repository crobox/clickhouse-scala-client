package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

import scala.annotation.implicitNotFound




object DateTimeFunctions {
  @implicitNotFound("DateFunction can only be used on column of type LocalDate or DateTime")
  type DateOrDateTime[T] = Union[T, LocalDate with DateTime]
  type DateOrDateTimeCol[T] = Union[T, TableColumn[LocalDate] with TableColumn[DateTime]]

  abstract class DateTimeFunction[V : DateOrDateTimeCol,O](val ddt: V)
    extends ExpressionColumn[V](ddt.asInstanceOf[TableColumn[O]])

  case class Year[V : DateOrDateTimeCol](d: V) extends DateTimeFunction[V,Int](d)
  case class Month[V : DateOrDateTimeCol](d: V) extends DateTimeFunction[V,Int](d)
  case class DayOfMonth[V : DateOrDateTimeCol](d: V) extends DateTimeFunction[V,Int](d)


  trait DateTimeFunctionsDsl {
    def toYear[T: DateOrDateTime](col: TableColumn[T]) = col match {
      case c: TableColumn[LocalDate] => Year(c)
      case c: TableColumn[DateTime] => Year(c)
    }

    def toMonth[T: DateOrDateTime](col: TableColumn[T]) = col match {
      case c: TableColumn[LocalDate] => Month(c)
      case c: TableColumn[DateTime] => Month(c)
    }

    def toDayOfMonth[T: DateOrDateTime](col: TableColumn[T]) = col match {
      case c: TableColumn[LocalDate] => DayOfMonth(c)
      case c: TableColumn[DateTime] => DayOfMonth(c)
    }
  }
}
