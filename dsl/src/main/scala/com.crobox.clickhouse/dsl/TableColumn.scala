package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.AggregateFunction.AggregationFunctionsDsl
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.marshalling.QueryValue
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType.SimpleColumnType
import com.crobox.clickhouse.dsl.schemabuilder.{ColumnType, DefaultValue}
import com.dongxiguo.fastring.Fastring.Implicits._

sealed case class EmptyColumn() extends TableColumn("")

trait Column {
  val name: String
}

class TableColumn[V](val name: String) extends Column {

  def as(alias: String): AliasedColumn[V] =
    AliasedColumn(this, alias)

  def aliased(alias: String): AliasedColumn[V] =
    AliasedColumn(this, alias)

  def as[C <: TableColumn[V]](alias: C): AliasedColumn[V] = AliasedColumn(this, alias.name)
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

abstract class TypeCastColumn[V](targetColumn: AnyTableColumn) extends ExpressionColumn[V](targetColumn)

case class Reinterpret[V](typeCastColumn: TypeCastColumn[V] with Reinterpretable)
    extends TypeCastColumn[V](typeCastColumn)


//FIXME: Here until we add them to the DSL trough the vocabulary update (feature/voc-typed-magnets)
abstract class ArithmeticFunction[V]() extends ExpressionColumn[V](EmptyColumn())
case class Multiply[V](left: AnyTableColumn, right: AnyTableColumn) extends ArithmeticFunction[V]
case class Divide[V](left: AnyTableColumn, right: AnyTableColumn) extends ArithmeticFunction[V]
case class Plus[V](left: AnyTableColumn, right: AnyTableColumn) extends ArithmeticFunction[V]
case class Minus[V](left: AnyTableColumn, right: AnyTableColumn) extends ArithmeticFunction[V]

//Tagging of compatible
sealed trait Reinterpretable

case class UInt8(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable
case class UInt16(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable
case class UInt32(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable
case class UInt64(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable

case class Int8(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable
case class Int16(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable
case class Int32(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable
case class Int64(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Long](tableColumn)
    with Reinterpretable

case class Float32(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Float](tableColumn)
    with Reinterpretable
case class Float64(tableColumn: AnyTableColumn, orZero: Boolean = false)
    extends TypeCastColumn[Float](tableColumn)
    with Reinterpretable

case class DateRep(tableColumn: AnyTableColumn)
    extends TypeCastColumn[org.joda.time.LocalDate](tableColumn)
    with Reinterpretable
case class DateTimeRep(tableColumn: AnyTableColumn)
    extends TypeCastColumn[org.joda.time.DateTime](tableColumn)
    with Reinterpretable

case class StringRep(tableColumn: AnyTableColumn)           extends TypeCastColumn[String](tableColumn) with Reinterpretable
case class FixedString(tableColumn: AnyTableColumn, n: Int) extends TypeCastColumn[String](tableColumn)
case class StringCutToZero(tableColumn: AnyTableColumn)    extends TypeCastColumn[String](tableColumn)

case class Cast[V](tableColumn: TableColumn[V], simpleColumnType: SimpleColumnType)
    extends TypeCastColumn[V](tableColumn)

case class LowerCaseColumn(tableColumn: AnyTableColumn) extends ExpressionColumn[String](tableColumn)

case class All() extends ExpressionColumn[Long](EmptyColumn())

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

trait ColumnOperations extends AggregationFunctionsDsl with TypeCastColumnOperations with ArithmeticOperations{
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

  def arrayJoin[V](tableColumn: TableColumn[Seq[V]]) =
    ArrayJoin(tableColumn)

  def tuple[T1, T2](firstColumn: TableColumn[T1], secondColumn: TableColumn[T2]) =
    TupleColumn[(T1, T2)](firstColumn, secondColumn)

  def lowercase(tableColumn: TableColumn[String]) =
    LowerCaseColumn(tableColumn)

}

trait ArithmeticOperations {
  def multiply[V](left: AnyTableColumn, right: AnyTableColumn) = Multiply[V](left, right)
  def divide[V](left: AnyTableColumn, right: AnyTableColumn) = Divide[V](left, right)
  def plus[V](left: AnyTableColumn, right: AnyTableColumn) = Plus[V](left, right)
  def minus[V](left: AnyTableColumn, right: AnyTableColumn) = Minus[V](left, right)
}

trait TypeCastColumnOperations {

  def toUInt8(tableColumn: AnyTableColumn) = UInt8(tableColumn)
  def toUInt16(tableColumn: AnyTableColumn) = UInt16(tableColumn)
  def toUInt32(tableColumn: AnyTableColumn) = UInt32(tableColumn)
  def toUInt64(tableColumn: AnyTableColumn) = UInt64(tableColumn)

  def toInt8(tableColumn: AnyTableColumn) = Int8(tableColumn)
  def toInt16(tableColumn: AnyTableColumn) = Int16(tableColumn)
  def toInt32(tableColumn: AnyTableColumn) = Int32(tableColumn)
  def toInt64(tableColumn: AnyTableColumn) = Int64(tableColumn)

  def toFloat32(tableColumn: AnyTableColumn) = Float32(tableColumn)
  def toFloat64(tableColumn: AnyTableColumn) = Float64(tableColumn)

  def toUInt8OrZero(tableColumn: AnyTableColumn) = UInt8(tableColumn, true)
  def toUInt16OrZero(tableColumn: AnyTableColumn) = UInt16(tableColumn, true)
  def toUInt32OrZero(tableColumn: AnyTableColumn) = UInt32(tableColumn, true)
  def toUInt64OrZero(tableColumn: AnyTableColumn) = UInt64(tableColumn, true)

  def toInt8OrZero(tableColumn: AnyTableColumn) = Int8(tableColumn, true)
  def toInt16OrZero(tableColumn: AnyTableColumn) = Int16(tableColumn, true)
  def toInt32OrZero(tableColumn: AnyTableColumn) = Int32(tableColumn, true)
  def toInt64OrZero(tableColumn: AnyTableColumn) = Int64(tableColumn, true)

  def toFloat32OrZero(tableColumn: AnyTableColumn) = Float32(tableColumn, true)
  def toFloat64OrZero(tableColumn: AnyTableColumn) = Float64(tableColumn, true)

  def toDate(tableColumn: AnyTableColumn) = DateRep(tableColumn)
  def toDateTime(tableColumn: AnyTableColumn) = DateTimeRep(tableColumn)

  def toStringRep(tableColumn: AnyTableColumn) = StringRep(tableColumn)
  def toFixedString(tableColumn: AnyTableColumn, n: Int) = FixedString(tableColumn, n)
  def toStringCutToZero(tableColumn: AnyTableColumn) = StringCutToZero(tableColumn)

  def reinterpret[V](typeCastColumn: TypeCastColumn[V] with Reinterpretable) = Reinterpret(typeCastColumn)

  def cast(tableColumn: TableColumn[Long], simpleColumnType: SimpleColumnType) = Cast(tableColumn, simpleColumnType)
}

object ColumnOperations extends ColumnOperations {}
