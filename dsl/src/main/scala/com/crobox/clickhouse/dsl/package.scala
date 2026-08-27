package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.column.ClickhouseColumnFunctions
import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}

import scala.util.Try

package object dsl extends ClickhouseColumnFunctions with QueryFactory with QueryValueFormats with TableFunctions {

  /**
   * Exposes the OperationalQuery.+ operator on Try[OperationalQuery]
   */
  implicit class OperationalQueryTryLifter(base: Try[OperationalQuery]) {

    def +(other: OperationalQuery): Try[OperationalQuery] =
      for {
        b  <- base
        bo <- b + other
      } yield OperationalQuery(bo.internalQuery)

    def +(other: Try[OperationalQuery]): Try[OperationalQuery] =
      for {
        b  <- base
        o  <- other
        bo <- b + o
      } yield OperationalQuery(bo.internalQuery)
  }

  implicit val booleanNumeric: Numeric[Boolean] = new Numeric[Boolean] {
    override def plus(x: Boolean, y: Boolean): Boolean = x || y

    override def minus(x: Boolean, y: Boolean): Boolean = x ^ y

    override def times(x: Boolean, y: Boolean): Boolean = x && y

    override def negate(x: Boolean): Boolean = !x

    override def fromInt(x: Int): Boolean = if (x <= 0) false else true

    override def toInt(x: Boolean): Int = if (x) 1 else 0

    override def toLong(x: Boolean): Long = if (x) 1 else 0

    override def toFloat(x: Boolean): Float = if (x) 1 else 0

    override def toDouble(x: Boolean): Double = if (x) 1 else 0

    def parseString(str: String): Option[Boolean] = Try(str == "1").toOption

    override def compare(x: Boolean, y: Boolean): Int = (x, y) match {
      case (false, true) => -1
      case (true, false) => 1
      case _             => 0
    }

  }

  def conditional(column: Column, condition: Boolean): Column =
    if (condition) column else EmptyColumn

  def ref[V](refName: String): RefColumn[V] = RefColumn[V](refName)

  def const[V: QueryValue](const: V): Const[V] = Const(const)

  def raw(rawSql: String): RawColumn = RawColumn(rawSql)

  def all: All = All()

  def columnCase[V](condition: TableColumn[Boolean], result: TableColumn[V]): Case[V] = Case[V](condition, result)

  /**
   * Both build the node even with no cases, where they once handed the default straight back. That short-circuit is
   * what forced the return type up to `TableColumn`, and a lambda -- `arrayMap`'s and every other higher-order one --
   * asks for an `ExpressionColumn`, so every conditional inside one needed a cast the compiler could not check.
   *
   * The degenerate case is not lost, only moved: the tokenizer renders a caseless conditional as the bare default, so
   * `multiIf(x)` is still `x`.
   *
   * `Conditional` rather than `ExpressionColumn` because a caller building one up -- adding a case to a conditional it
   * already has -- needs the node, and every other builder here hands its own node back too.
   */
  def switch[V](defaultValue: TableColumn[V], cases: Case[V]*): Conditional[V] =
    Conditional(cases, defaultValue, multiIf = false)

  def multiIf[V](defaultValue: TableColumn[V], cases: Case[V]*): Conditional[V] =
    Conditional(cases, defaultValue, multiIf = true)
}
