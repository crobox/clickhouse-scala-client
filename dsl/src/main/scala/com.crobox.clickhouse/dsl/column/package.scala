package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.marshalling.QueryValue

package object column {
  implicit val booleanNumeric: Numeric[Boolean] = new Numeric[Boolean] {
    override def plus(x: Boolean, y: Boolean) = x || y

    override def minus(x: Boolean, y: Boolean) = x ^ y

    override def times(x: Boolean, y: Boolean) = x && y

    override def negate(x: Boolean) = !x

    override def fromInt(x: Int) = if (x <= 0) false else true

    override def toInt(x: Boolean) = if (x) 1 else 0

    override def toLong(x: Boolean) = if (x) 1 else 0

    override def toFloat(x: Boolean) = if (x) 1 else 0

    override def toDouble(x: Boolean) = if (x) 1 else 0

    override def compare(x: Boolean, y: Boolean) = ???
  }

  def conditional(column: AnyTableColumn, condition: Boolean) =
    if (condition) column else EmptyColumn()

  def ref[V](refName: String) =
    new RefColumn[V](refName)

  def const[V: QueryValue](const: V) =
    Const(const)

  def rawColumn(tableColumn: AnyTableColumn) =
    RawColumn(tableColumn)

  def all() =
    All()

  def switch[V](defaultValue: TableColumn[V], cases: Case[V]*) =
    Conditional(cases, defaultValue)

  def columnCase[V](condition: Comparison, value: TableColumn[V]) = Case[V](value, condition)

  def tuple[T1, T2](firstColumn: TableColumn[T1], secondColumn: TableColumn[T2]) =
    TupleColumn[(T1, T2)](firstColumn, secondColumn)

  def lowercase(tableColumn: TableColumn[String]) =
    LowerCaseColumn(tableColumn)
}
