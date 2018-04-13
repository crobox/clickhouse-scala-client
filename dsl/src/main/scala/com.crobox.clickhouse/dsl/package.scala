package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.execution.{ClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.dsl.marshalling.{QueryValue, QueryValueFormats}
import com.dongxiguo.fastring.Fastring.Implicits._
import spray.json.{JsonReader, JsonWriter}

import scala.annotation.implicitNotFound
import scala.collection.immutable.Iterable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

package object dsl extends ColumnOperations with QueryFactory with QueryValueFormats {

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

  implicit class StringColumnWithCondition(column: TableColumn[String]) {

    //TODO switch the starts/ends/contains for iterables to use a one if for lists
    def startsWith(others: Iterable[String])(implicit ev: QueryValue[String],
                                             validator: ComparisonValidator[Iterable[_ >: String]] =
                                               ColumnsWithCondition.notEmptyValidator): Comparison =
      isLike(others.map(other => fast"$other%".toString))

    def endsWith(others: Iterable[String])(implicit ev: QueryValue[String],
                                           validator: ComparisonValidator[Iterable[_ >: String]] =
                                             ColumnsWithCondition.notEmptyValidator): Comparison =
      isLike(others.map(other => fast"%$other".toString))

    def contains(others: Iterable[String])(implicit ev: QueryValue[String],
                                           validator: ComparisonValidator[Iterable[_ >: String]] =
                                             ColumnsWithCondition.notEmptyValidator): Comparison =
      isLike(others.map(other => fast"%$other%".toString))

    def startsWith(other: String)(implicit ev: QueryValue[String]): Comparison =
      isLike(fast"$other%")

    def endsWith(other: String)(implicit ev: QueryValue[String]): Comparison =
      isLike(fast"%$other")

    def contains(other: String)(implicit ev: QueryValue[String]): Comparison =
      isLike(fast"%$other%")

    def isLike(other: Iterable[String])(implicit ev: QueryValue[String],
                                        validator: ComparisonValidator[Iterable[_ >: String]] =
                                          ColumnsWithCondition.notEmptyValidator): Comparison =
      if (validator.isValid(other)) {
        other
          .map { o =>
            column.isLike(o)
          }
          .reduce { (a, b) =>
            a or b
          }
      } else NoOpComparison()

    def isLike(other: String)(implicit ev: QueryValue[String]): Comparison =
      ValueColumnComparison[String, String](column, "LIKE", other)(ev)

    def empty(): Comparison =
      FunctionColumnComparison("empty", column)

    def notEmpty(): Comparison =
      FunctionColumnComparison("notEmpty", column)

  }

  implicit class BooleanConditions(column: TableColumn[Boolean]) {

    def isFalse(implicit qv: QueryValue[Boolean]): Comparison =
      column.isEq(false)

    def isTrue(implicit qv: QueryValue[Boolean]): Comparison =
      column.isEq(true)
  }

  implicit class ColumnsWithCondition[V](column: TableColumn[V]) {

    def <(other: TableColumn[V]): Comparison =
      ColRefColumnComparison[V](column, "<", other)

    def <(other: V)(implicit ev: QueryValue[V]): Comparison =
      ValueColumnComparison[V, V](column, "<", other)

    def >(other: TableColumn[V]): Comparison =
      ColRefColumnComparison[V](column, ">", other)

    def >(other: V)(implicit ev: QueryValue[V]): Comparison =
      ValueColumnComparison[V, V](column, ">", other)

    def <=(other: TableColumn[V]): Comparison =
      ColRefColumnComparison(column, "<=", other)

    def <=(other: V)(implicit ev: QueryValue[V]): Comparison =
      ValueColumnComparison[V, V](column, "<=", other)

    def >=(other: TableColumn[V]): Comparison =
      ColRefColumnComparison(column, ">=", other)

    def >=(other: V)(implicit ev: QueryValue[V]): Comparison =
      ValueColumnComparison[V, V](column, ">=", other)

    def isEq(other: TableColumn[V]): Comparison =
      ColRefColumnComparison(column, "=", other)

    def isEq(other: V)(implicit ev: QueryValue[V],
                       validator: ComparisonValidator[V] = ColumnsWithCondition.defaultValidator): Comparison =
      if (validator.isValid(other))
        ValueColumnComparison[V, V](column, "=", other)
      else
        NoOpComparison()

    def isIn(other: Iterable[V])(implicit ev: QueryValue[V],
                                 validator: ComparisonValidator[Iterable[_ >: V]] =
                                   ColumnsWithCondition.notEmptyValidator): Comparison =
      if (validator.isValid(other))
        ValueColumnComparison[V, Iterable[V]](column, "IN", other)(QueryValueFormats.queryValueToSeq[V](ev))
      else
        NoOpComparison()

  }

  implicit class ArrayColumnConditions[V](column: TableColumn[Seq[V]]) {

    def notEmpty(): Comparison =
      FunctionColumnComparison("notEmpty", column)

    def empty(): Comparison =
      FunctionColumnComparison("empty", column)

    def exists(condition: (TableColumn[V] => Comparison)) = {
      val conditionColumn = ColumnOperations.ref[V]("x")
      HigherOrderFunction("arrayExists", conditionColumn, condition(conditionColumn), column)
    }

  }

  implicit class NegatedStringConditions(negated: NegateColumn[String])
      extends StringColumnWithCondition(negated.column) {

    override def isLike(other: String)(implicit ev: QueryValue[String]): Comparison =
      ValueColumnComparison[String, String](negated.column, "NOT LIKE", other)(ev)

    override def isLike(other: Iterable[String])(implicit ev: QueryValue[String],
                                                 validator: ComparisonValidator[Iterable[_ >: String]]): Comparison =
      if (validator.isValid(other)) {
        other
          .map { o =>
            negated.isLike(o)
          }
          .reduce { (a, b) =>
            a and b
          }
      } else NoOpComparison()

  }

  implicit class NegatedConditions[V](negated: NegateColumn[V]) extends ColumnsWithCondition(negated.column) {

    override def isEq(other: V)(implicit ev: QueryValue[V], validator: ComparisonValidator[V]): Comparison =
      if (validator.isValid(other))
        ValueColumnComparison[V, V](negated.column, "!=", other)
      else
        NoOpComparison()

    override def isIn(other: Iterable[V])(implicit ev: QueryValue[V],
                                          validator: ComparisonValidator[Iterable[_ >: V]]): Comparison =
      super.isIn(other) match {
        case comparison: ValueColumnComparison[_, V] => comparison.copy(operator = "NOT IN")
        case other: Comparison                       => other
      }
  }

  case class NegateColumn[V](column: TableColumn[V])

  implicit class NegateComparison[V](column: TableColumn[V]) {

    def not(): NegateColumn[V] =
      NegateColumn(column)
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

    implicit val notEmptyValidator = new NotEmptyValidator[Iterable[_]]
  }

  implicit class ChainableColumns(columnComparison: Comparison) {

    def and(other: Comparison): Comparison =
      (columnComparison, other) match {
        case (_: NoOpComparison, _: NoOpComparison) => NoOpComparison()
        case _                                      => ChainableColumnCondition(columnComparison, "AND", other)
      }

    def or(other: Comparison): Comparison =
      (columnComparison, other) match {
        case (_: NoOpComparison, _: NoOpComparison) => NoOpComparison()
        case _                                      => ChainableColumnCondition(columnComparison, "OR", other)
      }
  }

  implicit def optionToCondition(option: Option[Comparison]): Comparison =
    option.getOrElse(NoOpComparison())

  implicit def toComparison(comparisons: Seq[Comparison]): Comparison =
    comparisons.reduceOption(_ and _)

  abstract class Comparison extends TableColumn[Boolean]("")

  case class ChainableColumnCondition(left: Comparison, operator: String, right: Comparison) extends Comparison

  abstract class ColumnComparison[V, W](left: TableColumn[V], operator: String) extends Comparison {
    val right: W
  }

  case class ValueColumnComparison[V, W](left: TableColumn[V], operator: String, override val right: W)(
      implicit ev: QueryValue[W]
  ) extends ColumnComparison[V, W](left: TableColumn[V], operator: String) {
    val queryValueEvidence: QueryValue[W] = ev
  }

  case class ColRefColumnComparison[V](left: TableColumn[V], operator: String, override val right: TableColumn[V])
      extends ColumnComparison[V, TableColumn[V]](left: TableColumn[V], operator: String)

  case class FunctionColumnComparison[V](functionName: String, column: TableColumn[V]) extends Comparison

  case class HigherOrderFunction[V](functionName: String,
                                    conditionColumn: AnyTableColumn,
                                    comparison: Comparison,
                                    column: TableColumn[Seq[V]])
      extends Comparison

  case class NoOpComparison() extends Comparison
}
