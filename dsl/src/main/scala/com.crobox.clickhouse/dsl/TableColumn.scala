package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.clickhouse.QueryValue
import com.crobox.clickhouse.time.MultiInterval
import org.joda.time.DateTime

sealed case class EmptyColumn() extends TableColumn("")

abstract class TableColumn[V](val name: String) {

  def as(alias: String): AliasedColumn[V] =
    AliasedColumn(this, alias)

  def as(alias: TableColumn[V]): AliasedColumn[V] =
    AliasedColumn(this, alias.name)
}

object TableColumn {
  type AnyTableColumn = TableColumn[_]
}
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

case class RefColumn[V](ref: String) extends TableColumn[V](ref)

case class AliasedColumn[V](original: TableColumn[V], alias: String) extends TableColumn[V](alias)

case class TupleColumn[V](elements: AnyTableColumn*) extends TableColumn[V]("")

abstract class ExpressionColumn[V](targetColumn: AnyTableColumn) extends TableColumn[V](targetColumn.name)

case class Count[T <: Table]() extends ExpressionColumn[Long](EmptyColumn())

case class CountIf[T <: Table](expressionColumn: ExpressionColumn[_]) extends ExpressionColumn[Long](EmptyColumn())

case class UniqIf[T <: Table](tableColumn: AnyTableColumn, expressionColumn: ExpressionColumn[_])
    extends ExpressionColumn[Long](tableColumn)

case class ArrayJoin[V](tableColumn: TableColumn[Seq[V]]) extends ExpressionColumn[V](tableColumn)

case class GroupUniqArray[V](tableColumn: TableColumn[V]) extends ExpressionColumn[Seq[V]](tableColumn)

case class UInt64[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class Uniq[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class UniqState[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[String](tableColumn)

case class UniqMerge[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class Sum[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class Min[V](tableColumn: TableColumn[V]) extends ExpressionColumn[V](tableColumn)

case class Max[V](tableColumn: TableColumn[V]) extends ExpressionColumn[V](tableColumn)

case class Empty[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[Boolean](tableColumn)

case class NotEmpty[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[Boolean](tableColumn)

case class LowerCaseColumn[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[String](tableColumn)

case class TimeSeries[T <: Table](tableColumn: TableColumn[Long],
                                  interval: MultiInterval,
                                  dateColumn: Option[TableColumn[DateTime]])
    extends ExpressionColumn[Long](tableColumn)

case class All[T <: Table]() extends ExpressionColumn[Long](EmptyColumn())
//TODO allow comparisons to be used in expressions
case class BooleanInt[T <: Table](tableColumn: AnyTableColumn, expected: Int)
    extends ExpressionColumn[Int](tableColumn)

case class Case[V](column: TableColumn[V], condition: Comparison)

case class Conditional[V](cases: Seq[Case[V]], default: AnyTableColumn) extends ExpressionColumn[V](EmptyColumn())

/**
 * Used when referencing to a column in an expression
 */
case class RawColumn[T <: Table](tableColumn: AnyTableColumn) extends ExpressionColumn[Boolean](tableColumn)

/**
 * Parse the supplied value as a constant value column in the query
 */
case class Const[V: QueryValue](const: V) extends ExpressionColumn[V](EmptyColumn()) {
  val parsed = implicitly[QueryValue[V]].apply(const)
}

trait ColumnOperations {

  def conditional[T <: Table](column: AnyTableColumn, condition: Boolean): AnyTableColumn =
    if (condition) column else EmptyColumn()

  def ref[V](refName: String): TableColumn[V] =
    new RefColumn[V](refName)

  def const[V: QueryValue](const: V): Const[V] =
    Const(const)

  def toUInt64[T <: Table](tableColumn: TableColumn[Long]): UInt64[T] =
    UInt64(tableColumn)

  def count[T <: Table](): TableColumn[Long] =
    Count()

  def countIf[T <: Table](expressionColumn: ExpressionColumn[_]): TableColumn[Long] =
    CountIf(expressionColumn)

  def uniqIf[T <: Table](tableColumn: AnyTableColumn, condition: ExpressionColumn[_]): TableColumn[Long] =
    UniqIf(tableColumn, condition)

  def notEmpty[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Boolean] =
    NotEmpty(tableColumn)

  def empty[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Boolean] =
    Empty(tableColumn)

  def is[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Int] =
    BooleanInt(tableColumn, 1)

  def not[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Int] =
    BooleanInt(tableColumn, 0)

  def rawColumn[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Boolean] =
    RawColumn(tableColumn)

  def uniq[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Long] =
    Uniq(tableColumn)

  def uniqState[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[String] =
    UniqState(tableColumn)

  def uniqMerge[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Long] =
    UniqMerge(tableColumn)

  def sum[T <: Table](tableColumn: AnyTableColumn): ExpressionColumn[Long] =
    Sum(tableColumn)

  def min[V](tableColumn: TableColumn[V]): ExpressionColumn[V] =
    Min(tableColumn)

  def max[V](tableColumn: TableColumn[V]): ExpressionColumn[V] =
    Max(tableColumn)

  def all(): All[Nothing] =
    All()

  def switch[V](defaultValue: TableColumn[V], cases: Case[V]*): TableColumn[V] =
    Conditional(cases, defaultValue)

  def columnCase[V](condition: Comparison, value: TableColumn[V]) = Case[V](value, condition)

  /*
   * @dateColumn Optional column
   * */
  def timeSeries[T <: Table](tableColumn: TableColumn[Long],
                             interval: MultiInterval,
                             dateColumn: Option[TableColumn[DateTime]] = None): ExpressionColumn[Long] =
    TimeSeries(tableColumn, interval, dateColumn)

  def arrayJoin[V](tableColumn: TableColumn[Seq[V]]): ExpressionColumn[V] =
    ArrayJoin(tableColumn)

  def groupUniqArray[V](tableColumn: TableColumn[V]): ExpressionColumn[Seq[V]] =
    GroupUniqArray(tableColumn)

  def tuple[T1, T2](firstColumn: TableColumn[T1], secondColumn: TableColumn[T2]) =
    TupleColumn[(T1, T2)](firstColumn, secondColumn)

  def lowercase[T <: Table](tableColumn: TableColumn[String]): ExpressionColumn[String] =
    LowerCaseColumn(tableColumn)

}

object ColumnOperations extends ColumnOperations {}
