package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.marshalling.QueryValue
import com.crobox.clickhouse.dsl.schemabuilder.{ColumnType, DefaultValue}

case object EmptyColumn extends TableColumn("NULL")

trait Column {
  val name: String
  lazy val quoted: String = ClickhouseStatement.quoteIdentifier(name)
}

abstract class TableColumn[+V](val name: String) extends Column {

  def as(alias: String): AliasedColumn[V] =
    AliasedColumn(this, alias)

  def aliased(alias: String): AliasedColumn[V] =
    AliasedColumn(this, alias)

  def as[C <: Column](alias: C): AliasedColumn[V] = AliasedColumn(this, alias.name)
}

case class NativeColumn[V](override val name: String,
                           clickhouseType: ColumnType = ColumnType.String,
                           defaultValue: DefaultValue = DefaultValue.NoDefault)
  extends TableColumn[V](name) {

  def query: String = s"$quoted $clickhouseType$defaultValue".toString
}

object TableColumn {
  type AnyTableColumn = Column
}

case class RefColumn[V](ref: String) extends TableColumn[V](ref)

case class AliasedColumn[+V](original: TableColumn[V], alias: String) extends TableColumn[V](alias)

case class TupleColumn[V](elements: AnyTableColumn*) extends TableColumn[V](EmptyColumn.name)

abstract class ExpressionColumn[+V](targetColumn: AnyTableColumn) extends TableColumn[V](targetColumn.name)

case class All() extends ExpressionColumn[Long](EmptyColumn)

case class Case[V](condition: TableColumn[Boolean], result: TableColumn[V])

case class Conditional[V](cases: Seq[Case[V]], default: AnyTableColumn) extends ExpressionColumn[V](EmptyColumn)

/**
  * Used when referencing to a column in an expression
  */
case class RawColumn(rawSql: String) extends ExpressionColumn[Boolean](EmptyColumn)

/**
  * Parse the supplied value as a constant value column in the query
  */
case class Const[V: QueryValue](const: V) extends ExpressionColumn[V](EmptyColumn) {
  val parsed = implicitly[QueryValue[V]].apply(const)
}


