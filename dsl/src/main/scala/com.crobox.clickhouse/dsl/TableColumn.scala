package com.crobox.clickhouse.dsl

import com.dongxiguo.fastring.Fastring.Implicits._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.marshalling.QueryValue
import com.crobox.clickhouse.dsl.schemabuilder.{ColumnType, DefaultValue}
import com.crobox.clickhouse.time.MultiInterval
import org.joda.time.DateTime

sealed case class EmptyColumn() extends TableColumn("")

trait Column {
  val name: String
}

class TableColumn[V](val name: String) extends Column {

  def as(alias: String): AliasedColumn[V] =
    AliasedColumn(this, alias)

  def as(alias: TableColumn[V]): AliasedColumn[V] =
    AliasedColumn(this, alias.name)
}

case class NativeColumn[V](override val name: String,
                           clickhouseType: ColumnType = ColumnType.String,
                           defaultValue: DefaultValue = DefaultValue.NoDefault)
    extends TableColumn[V](name) {
  require(ClickhouseStatement.isValidIdentifier(name), "Invalid column name identifier")

  def query(): String = fast"$name $clickhouseType$defaultValue".toString
}

object TableColumn {
  type AnyTableColumn = Column
}


case class RefColumn[V](ref: String) extends TableColumn[V](ref)

case class AliasedColumn[V](original: TableColumn[V], alias: String) extends TableColumn[V](alias)

case class TupleColumn[V](elements: AnyTableColumn*) extends TableColumn[V]("")

abstract class ExpressionColumn[V](targetColumn: AnyTableColumn) extends TableColumn[V](targetColumn.name)

case class Count(column: Option[TableColumn[_]] = None) extends ExpressionColumn[Long](EmptyColumn())

case class CountIf(expressionColumn: ExpressionColumn[_]) extends ExpressionColumn[Long](EmptyColumn())

case class UniqIf(tableColumn: AnyTableColumn, expressionColumn: ExpressionColumn[_])
    extends ExpressionColumn[Long](tableColumn)

case class ArrayJoin[V](tableColumn: TableColumn[Seq[V]]) extends ExpressionColumn[V](tableColumn)

case class GroupUniqArray[V](tableColumn: TableColumn[V]) extends ExpressionColumn[Seq[V]](tableColumn)

case class UInt64(tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class Uniq(tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class UniqState(tableColumn: AnyTableColumn) extends ExpressionColumn[String](tableColumn)

case class UniqMerge(tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class Sum(tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class Min[V](tableColumn: TableColumn[V]) extends ExpressionColumn[V](tableColumn)

case class Max[V](tableColumn: TableColumn[V]) extends ExpressionColumn[V](tableColumn)

case class Empty(tableColumn: AnyTableColumn) extends ExpressionColumn[Boolean](tableColumn)

case class NotEmpty(tableColumn: AnyTableColumn) extends ExpressionColumn[Boolean](tableColumn)

case class LowerCaseColumn(tableColumn: AnyTableColumn) extends ExpressionColumn[String](tableColumn)

case class TimeSeries(tableColumn: TableColumn[Long],
                      interval: MultiInterval,
                      dateColumn: Option[TableColumn[DateTime]])
    extends ExpressionColumn[Long](tableColumn)

case class All() extends ExpressionColumn[Long](EmptyColumn())
//TODO allow comparisons to be used in expressions
case class BooleanInt(tableColumn: AnyTableColumn, expected: Int) extends ExpressionColumn[Int](tableColumn)

case class Case[V](column: TableColumn[V], condition: Comparison)

case class Conditional[V](cases: Seq[Case[V]], default: AnyTableColumn) extends ExpressionColumn[V](EmptyColumn())

/**
 * Used when referencing to a column in an expression
 */
case class RawColumn(tableColumn: AnyTableColumn) extends ExpressionColumn[Boolean](tableColumn)

/**
 * Parse the supplied value as a constant value column in the query
 */
case class Const[V: QueryValue](const: V) extends ExpressionColumn[V](EmptyColumn()) {
  val parsed = implicitly[QueryValue[V]].apply(const)
}

trait ColumnOperations {

  def conditional(column: AnyTableColumn, condition: Boolean): AnyTableColumn =
    if (condition) column else EmptyColumn()

  def ref[V](refName: String): TableColumn[V] =
    new RefColumn[V](refName)

  def const[V: QueryValue](const: V): Const[V] =
    Const(const)

  def toUInt64(tableColumn: TableColumn[Long]): UInt64 =
    UInt64(tableColumn)

  def count(): TableColumn[Long] =
    Count()

  def count(column: TableColumn[_]): TableColumn[Long] =
    Count(Option(column))

  def countIf(expressionColumn: ExpressionColumn[_]): TableColumn[Long] =
    CountIf(expressionColumn)

  def uniqIf(tableColumn: AnyTableColumn, condition: ExpressionColumn[_]): TableColumn[Long] =
    UniqIf(tableColumn, condition)

  def notEmpty(tableColumn: AnyTableColumn): ExpressionColumn[Boolean] =
    NotEmpty(tableColumn)

  def empty(tableColumn: AnyTableColumn): ExpressionColumn[Boolean] =
    Empty(tableColumn)

  def is(tableColumn: AnyTableColumn): ExpressionColumn[Int] =
    BooleanInt(tableColumn, 1)

  def not(tableColumn: AnyTableColumn): ExpressionColumn[Int] =
    BooleanInt(tableColumn, 0)

  def rawColumn(tableColumn: AnyTableColumn): ExpressionColumn[Boolean] =
    RawColumn(tableColumn)

  def uniq(tableColumn: AnyTableColumn): ExpressionColumn[Long] =
    Uniq(tableColumn)

  def uniqState(tableColumn: AnyTableColumn): ExpressionColumn[String] =
    UniqState(tableColumn)

  def uniqMerge(tableColumn: AnyTableColumn): ExpressionColumn[Long] =
    UniqMerge(tableColumn)

  def sum(tableColumn: AnyTableColumn): ExpressionColumn[Long] =
    Sum(tableColumn)

  def min[V](tableColumn: TableColumn[V]): ExpressionColumn[V] =
    Min(tableColumn)

  def max[V](tableColumn: TableColumn[V]): ExpressionColumn[V] =
    Max(tableColumn)

  def all(): All =
    All()

  def switch[V](defaultValue: TableColumn[V], cases: Case[V]*): TableColumn[V] =
    Conditional(cases, defaultValue)

  def columnCase[V](condition: Comparison, value: TableColumn[V]): Case[V] = Case[V](value, condition)

  /*
   * @dateColumn Optional column
   * */
  def timeSeries(tableColumn: TableColumn[Long],
                 interval: MultiInterval,
                 dateColumn: Option[TableColumn[DateTime]] = None): ExpressionColumn[Long] =
    TimeSeries(tableColumn, interval, dateColumn)

  def arrayJoin[V](tableColumn: TableColumn[Seq[V]]): ExpressionColumn[V] =
    ArrayJoin(tableColumn)

  def groupUniqArray[V](tableColumn: TableColumn[V]): ExpressionColumn[Seq[V]] =
    GroupUniqArray(tableColumn)

  def tuple[T1, T2](firstColumn: TableColumn[T1], secondColumn: TableColumn[T2]): TupleColumn[(T1, T2)] =
    TupleColumn[(T1, T2)](firstColumn, secondColumn)

  def lowercase(tableColumn: TableColumn[String]): ExpressionColumn[String] =
    LowerCaseColumn(tableColumn)

}

object ColumnOperations extends ColumnOperations {}
