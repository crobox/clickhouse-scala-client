package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.AggregateFunction.AggregationFunctionsDsl
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.marshalling.QueryValue
import com.crobox.clickhouse.dsl.schemabuilder.{ColumnType, DefaultValue}
import com.dongxiguo.fastring.Fastring.Implicits._

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
  val emptyColumn = EmptyColumn()

}

case class RefColumn[V](ref: String) extends TableColumn[V](ref)

case class AliasedColumn[V](original: TableColumn[V], alias: String) extends TableColumn[V](alias)

case class TupleColumn[V](elements: AnyTableColumn*) extends TableColumn[V]("")

abstract class ExpressionColumn[V](targetColumn: AnyTableColumn) extends TableColumn[V](targetColumn.name)

case class UInt64(tableColumn: AnyTableColumn) extends ExpressionColumn[Long](tableColumn)

case class LowerCaseColumn(tableColumn: AnyTableColumn) extends ExpressionColumn[String](tableColumn)

case class All() extends ExpressionColumn[Long](EmptyColumn())

case class Case[V](column: TableColumn[V], condition: Comparison)

case class Conditional[V](cases: Seq[Case[V]], default: AnyTableColumn) extends ExpressionColumn[V](EmptyColumn())

/**
 * Used when referencing to a column in an expression
 */
case class RawColumn(tableColumn: AnyTableColumn)   extends ExpressionColumn[Boolean](tableColumn)

/**
 * Parse the supplied value as a constant value column in the query
 */
case class Const[V: QueryValue](const: V) extends ExpressionColumn[V](EmptyColumn()) {
  val parsed = implicitly[QueryValue[V]].apply(const)
}

trait ColumnOperations extends AggregationFunctionsDsl {
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

  def toUInt64(tableColumn: TableColumn[Long]) =
    UInt64(tableColumn)

  def rawColumn(tableColumn: AnyTableColumn) =
    RawColumn(tableColumn)

  def all() =
    All()

  def switch[V](defaultValue: TableColumn[V], cases: Case[V]*) =
    Conditional(cases, defaultValue)

  def columnCase[V](condition: Comparison, value: TableColumn[V]) = Case[V](value, condition)


  def arrayJoin[V](tableColumn: TableColumn[Seq[V]]) =
    ArrayJoin(tableColumn)

  def tuple[T1, T2](firstColumn: TableColumn[T1], secondColumn: TableColumn[T2]) =
    TupleColumn[(T1, T2)](firstColumn, secondColumn)

  def lowercase(tableColumn: TableColumn[String]) =
    LowerCaseColumn(tableColumn)

}

object ColumnOperations extends ColumnOperations {}
