package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.column.ClickhouseColumnFunctions
import com.crobox.clickhouse.dsl.execution.{ClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.{ExecutionContext, Future}
import scala.math.BigDecimal
import scala.util.Try

package object dsl extends ClickhouseColumnFunctions with QueryFactory with QueryValueFormats {

  //Naive union type context bound
  trait Contra[-A]
  type Union[A, B] = Contra[A] <:< Contra[B]

  implicit class QueryExecution(query: Query) {

    def as[V: JsonReader](
        implicit executionContext: ExecutionContext,
        clickhouseExecutor: ClickhouseQueryExecutor
    ): Future[QueryResult[V]] = clickhouseExecutor.execute(query)
  }

  implicit class ValueInsertion[V: JsonWriter](values: Seq[V]) {

    def into(table: Table)(
        implicit executionContext: ExecutionContext,
        clickhouseExecutor: ClickhouseQueryExecutor
    ): Future[String] = clickhouseExecutor.insert(table, values)
  }

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
      case _ => 0
    }

  }

  def conditional(column: AnyTableColumn, condition: Boolean): AnyTableColumn =
    if (condition) column else EmptyColumn

  def ref[V](refName: String): RefColumn[V] = RefColumn[V](refName)

  def const[V: QueryValue](const: V): Const[V] = Const(const)

  def raw(rawSql: String): RawColumn = RawColumn(rawSql)

  def all(): All = All()

  def columnCase[V](condition: TableColumn[Boolean], result: TableColumn[V]): Case[V] = Case[V](condition, result)

  def switch[V](defaultValue: TableColumn[V], cases: Case[V]*): TableColumn[V] = cases match {
    case Nil => defaultValue
    case _ => Conditional(cases, defaultValue)
  }

}

