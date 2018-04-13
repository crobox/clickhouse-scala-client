package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.AggregateFunction.Combinator
import com.crobox.clickhouse.dsl.AnyResult.{AnyModifier, AnyResultDsl}
import com.crobox.clickhouse.dsl.Leveled.LevelModifier
import com.crobox.clickhouse.dsl.Sum.{SumDsl, SumModifier}
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.Uniq.{UniqDsl, UniqModifier}
import com.crobox.clickhouse.time.MultiInterval
import org.joda.time.DateTime

//https://clickhouse.yandex/docs/en/agg_functions/reference
abstract class AggregateFunction[V](targetColumn: AnyTableColumn) extends ExpressionColumn[V](targetColumn)

case class CombinedAggregatedFunction[T <: TableColumn[_], Res](combinator: Combinator[T, Res],
                                                                target: AggregateFunction[_])
    extends AggregateFunction[Res](EmptyColumn())

object AggregateFunction {
  type StateResult = String
  sealed trait Combinator[T <: Column, Result]

  case class If[T <: Column, Res](condition: Comparison)      extends Combinator[T, Res]
  case class CombinatorArray[T <: TableColumn[Seq[V]], V]()   extends Combinator[T, V]
  case class ArrayForEach[T <: TableColumn[Seq[Seq[V]]], V]() extends Combinator[T, Seq[V]]
  case class State[T <: Column]()                             extends Combinator[T, StateResult]
  case class Merge[T <: TableColumn[StateResult], Res]()      extends Combinator[T, Res]

  trait AggregationFunctionsCombinersDsl {

    def aggIf[T <: TableColumn[Res], Res](condition: Comparison)(aggregated: AggregateFunction[Res]) =
      CombinedAggregatedFunction(If(condition), aggregated)

    def array[T <: TableColumn[Seq[Res]], Res](aggregated: AggregateFunction[Res]) =
      CombinedAggregatedFunction(CombinatorArray[T, Res](), aggregated)

    def forEach[T <: TableColumn[Seq[Seq[V]]], V](aggregated: AggregateFunction[V]) =
      CombinedAggregatedFunction(ArrayForEach[T, V](), aggregated)

    def state[T <: TableColumn[_]](aggregated: AggregateFunction[_]) =
      CombinedAggregatedFunction(State[T](), aggregated)

    def merge[T <: TableColumn[StateResult], Res](aggregated: AggregateFunction[Res]) =
      CombinedAggregatedFunction(Merge[T, Res](), aggregated)
  }

  trait AggregationFunctionsDsl extends AggregationFunctionsCombinersDsl with UniqDsl with AnyResultDsl with SumDsl {

    def count() =
      Count()

    def count(column: TableColumn[_]) =
      Count(Option(column))

    def uniq(tableColumn: AnyTableColumn) =
      Uniq(tableColumn)

    def sum[T: Numeric](tableColumn: TableColumn[T]) =
      Sum(tableColumn)

    def min[V](tableColumn: TableColumn[V]) =
      Min(tableColumn)

    def max[V](tableColumn: TableColumn[V]) =
      Max(tableColumn)

    def timeSeries(tableColumn: TableColumn[Long],
                   interval: MultiInterval,
                   dateColumn: Option[TableColumn[DateTime]] = None) =
      TimeSeries(tableColumn, interval, dateColumn)

    def groupUniqArray[V](tableColumn: TableColumn[V]) =
      GroupUniqArray(tableColumn)

  }
}

case class Count(column: Option[AnyTableColumn] = None) extends AggregateFunction[Long](column.getOrElse(EmptyColumn()))

case class Uniq(tableColumn: AnyTableColumn, modifier: UniqModifier = Uniq.Normal)
    extends AggregateFunction[Long](tableColumn)

object Uniq {
  sealed trait UniqModifier
  case object Normal   extends UniqModifier
  case object Combined extends UniqModifier
  case object HLL12    extends UniqModifier
  case object Exact    extends UniqModifier

  trait UniqDsl {
    def combined(uniq: Uniq) = uniq.copy(modifier = Combined)
    def exact(uniq: Uniq)    = uniq.copy(modifier = Exact)
    def hll12(uniq: Uniq)    = uniq.copy(modifier = HLL12)
    def simple(uniq: Uniq)   = uniq.copy(modifier = Normal)
  }
}

case class AnyResult[T](tableColumn: TableColumn[T], modifier: AnyModifier = AnyResult.Normal)
    extends AggregateFunction[Long](tableColumn)

object AnyResult {
  sealed trait AnyModifier
  case object Normal extends AnyModifier
  case object Heavy  extends AnyModifier
  case object Last   extends AnyModifier

  trait AnyResultDsl {
    def heavy[T](any: AnyResult[T])  = any.copy(modifier = Heavy)
    def last[T](any: AnyResult[T])   = any.copy(modifier = Last)
    def simple[T](any: AnyResult[T]) = any.copy(modifier = Normal)
  }
}

case class Sum[T: Numeric](tableColumn: TableColumn[T], modifier: SumModifier = Sum.Normal)
    extends AggregateFunction[Long](tableColumn)

object Sum {
  sealed trait SumModifier
  case object Normal       extends SumModifier
  case object WithOverflow extends SumModifier
  case object Map          extends SumModifier

  trait SumDsl {
    def overflown[T: Numeric](sum: Sum[T]) = sum.copy(modifier = WithOverflow)
    def mapped[T: Numeric](sum: Sum[T])    = sum.copy(modifier = Map)
    def simple[T: Numeric](sum: Sum[T])    = sum.copy(modifier = Normal)
  }

}

case class Avg[T: Numeric](tableColumn: AnyTableColumn) extends AggregateFunction[Long](tableColumn)

case class ArrayJoin[V](tableColumn: TableColumn[Seq[V]]) extends ExpressionColumn[V](tableColumn)

case class GroupUniqArray[V](tableColumn: TableColumn[V]) extends AggregateFunction[Seq[V]](tableColumn)

case class GroupArray[V](tableColumn: TableColumn[V], maxValues: Option[Long])
    extends AggregateFunction[Seq[V]](tableColumn)

/*Works for numbers, dates, and dates with times. Returns: for numbers – Float64; for dates – a date; for dates with times – a date with time.Works for numbers, dates, and dates with times. Returns: for numbers – Float64; for dates – a date; for dates with times – a date with time.*/
case class Quantile[T](tableColumn: TableColumn[T],
                       modifier: LevelModifier,
                       level: Either[Float, Seq[Float]] = Left(0.5F))
    extends AggregateFunction[Long](tableColumn) {
  level match {
    case Left(quantileLevel) => require(quantileLevel >= 0 && quantileLevel <= 1)
    case Right(levels)       => levels.foreach(level => require(level >= 0 && level <= 1))
  }
}

case class Median[T](tableColumn: TableColumn[T], modifier: LevelModifier, level: Float = 0.5F)
    extends AggregateFunction[Long](tableColumn) {
  require(level > 0 && level < 1)
}

object Leveled {
  sealed trait LevelModifier
  case object Normal                                                 extends LevelModifier
  case object Exact                                                  extends LevelModifier
  case object TDigest                                                extends LevelModifier
  case class Deterministic[T: Numeric](determinator: TableColumn[T]) extends LevelModifier
  /*Works for numbers. Intended for calculating quantiles of page loading time in milliseconds.*/
  case object Timing extends LevelModifier
  /*The result is calculated as if the x value were passed weight number of times to the quantileTiming function.*/
  case class Weighted(weight: Int) extends LevelModifier {
    require(weight >= 0)
  }
  case class ExactWeighted(weight: Int) extends LevelModifier {
    require(weight >= 0)
  }
}

case class Min[V](tableColumn: TableColumn[V]) extends AggregateFunction[V](tableColumn)

case class Max[V](tableColumn: TableColumn[V]) extends AggregateFunction[V](tableColumn)

case class TimeSeries(tableColumn: TableColumn[Long],
                      interval: MultiInterval,
                      dateColumn: Option[TableColumn[DateTime]])
    extends AggregateFunction[Long](tableColumn)
