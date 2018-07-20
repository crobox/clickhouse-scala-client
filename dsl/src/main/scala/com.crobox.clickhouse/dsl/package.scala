package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.QueryFactory
import com.crobox.clickhouse.dsl.column.ClickhouseColumnFunctions
import com.crobox.clickhouse.dsl.execution.{ClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats
import com.dongxiguo.fastring.Fastring.Implicits._
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait DslLanguage  extends ClickhouseColumnFunctions with QueryFactory with QueryValueFormats
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

  trait ComparisonValidator[V] {

    def isValid(value: V): Boolean
  }

  object ColumnsWithCondition {

    case object NoValidator extends ComparisonValidator[Any] {

      override def isValid(value: Any): Boolean = true
    }

    class NotEmptyValidator[T] extends ComparisonValidator[T] {

      override def isValid(value: T): Boolean = value match {
        case x: Iterable[_] => x.nonEmpty
        case x: String      => x.nonEmpty
        case null           => false
        case _              => true
      }
    }

    implicit def defaultValidator[T] = new NotEmptyValidator[T]

    implicit val notEmptyValidator = new NotEmptyValidator[scala.Iterable[_]]
  }

}
