package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.QueryFactory
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.column.ClickhouseColumnFunctions
import com.crobox.clickhouse.dsl.execution.{ClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}
import com.dongxiguo.fastring.Fastring.Implicits._
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

sealed trait DslLanguage extends ClickhouseColumnFunctions with QueryFactory with QueryValueFormats
object DslLanguage extends DslLanguage

package object dsl extends DslLanguage {

  //Naive union type context bound
  trait Contra[-A]
  type Union[A, B] = Contra[A] <:< Contra[B]

  implicit def fstr2str(fstr: Fastring): String = fstr.toString

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

  def columnCase[V](condition: TableColumn[Boolean], result: TableColumn[V]): Case[V] = Case[V](condition, result)

  def switch[V](defaultValue: TableColumn[V], cases: Case[V]*): TableColumn[V] = cases match {
    case Nil => defaultValue
    case _ => Conditional(cases, defaultValue)
  }

}

