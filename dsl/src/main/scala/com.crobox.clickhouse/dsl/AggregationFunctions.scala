package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.AggregateFunction.Combinator
import com.crobox.clickhouse.dsl.AnyResult.{AnyModifier, AnyResultDsl}
import com.crobox.clickhouse.dsl.Leveled.{LevelModifier, LevelModifierDsl, LeveledAggregatedFunction}
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

  trait AggregationFunctionsDsl
      extends AggregationFunctionsCombinersDsl
      with UniqDsl
      with AnyResultDsl
      with SumDsl
      with LevelModifierDsl {

    def count() =
      Count()

    def count(column: TableColumn[_]) =
      Count(Option(column))

    def uniq(tableColumn: AnyTableColumn) =
      Uniq(tableColumn)

    def average[T: Numeric](tableColumn: TableColumn[T]) =
      Avg(tableColumn)

    def any[T](tableColumn: TableColumn[T]) =
      AnyResult(tableColumn)

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

    def median[V](target: TableColumn[V], level: Float = 0.5F) = Median(target, level = level)

    def quantile[V](target: TableColumn[V], level: Float = 0.5F) = Quantile(target, level = level)

    def quantiles[V](target: TableColumn[V], levels: Float*) = Quantiles(target, levels)

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
    extends AggregateFunction[T](tableColumn)

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
    extends AggregateFunction[Double](tableColumn)

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

case class Avg[T: Numeric](tableColumn: TableColumn[T]) extends AggregateFunction[Double](tableColumn)

case class ArrayJoin[V](tableColumn: TableColumn[Seq[V]]) extends ExpressionColumn[V](tableColumn)

case class GroupUniqArray[V](tableColumn: TableColumn[V]) extends AggregateFunction[Seq[V]](tableColumn)

case class GroupArray[V](tableColumn: TableColumn[V], maxValues: Option[Long])
    extends AggregateFunction[Seq[V]](tableColumn)

/*Works for numbers, dates, and dates with times. Returns: for numbers – Float64; for dates – a date; for dates with times – a date with time.Works for numbers, dates, and dates with times. Returns: for numbers – Float64; for dates – a date; for dates with times – a date with time.*/
case class Quantile[T](tableColumn: TableColumn[T], level: Float = 0.5F, modifier: LevelModifier = Leveled.Normal)
    extends LeveledAggregatedFunction[T](tableColumn) {
  require(level >= 0 && level <= 1)
}
case class Quantiles[T](tableColumn: TableColumn[T], levels: Seq[Float], modifier: LevelModifier = Leveled.Normal)
    extends LeveledAggregatedFunction[Seq[T]](tableColumn) {
  levels.foreach(level => require(level >= 0 && level <= 1))
}
case class Median[T](tableColumn: TableColumn[T], level: Float, modifier: LevelModifier = Leveled.Normal)
    extends LeveledAggregatedFunction[T](tableColumn) {
  require(level > 0 && level < 1)
}

object Leveled {
  sealed trait LevelModifier
  sealed abstract class LeveledAggregatedFunction[T](target: AnyTableColumn) extends AggregateFunction[T](target)
  case object Normal                                                         extends LevelModifier
  case object Exact                                                          extends LevelModifier
  case object TDigest                                                        extends LevelModifier
  case class Deterministic[T: Numeric](determinator: TableColumn[T])         extends LevelModifier
  /*Works for numbers. Intended for calculating quantiles of page loading time in milliseconds.*/
  case object Timing extends LevelModifier
  /*The result is calculated as if the x value were passed weight number of times to the quantileTiming function.*/
  case class TimingWeighted(weight: TableColumn[Int]) extends LevelModifier
  case class ExactWeighted(weight: TableColumn[Int])  extends LevelModifier

  trait LevelModifierDsl {

    def simple[T](aggregation: LeveledAggregatedFunction[T]): LeveledAggregatedFunction[T] =
      aggregation match {
        case median: Median[_]       => median.copy(modifier = Normal).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantile: Quantile[_]   => quantile.copy(modifier = Normal).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantiles: Quantiles[_] => quantiles.copy(modifier = Normal).asInstanceOf[LeveledAggregatedFunction[T]]
      }

    def exact[T](aggregation: LeveledAggregatedFunction[T]): LeveledAggregatedFunction[T] =
      aggregation match {
        case median: Median[_]       => median.copy(modifier = Exact).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantile: Quantile[_]   => quantile.copy(modifier = Exact).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantiles: Quantiles[_] => quantiles.copy(modifier = Exact).asInstanceOf[LeveledAggregatedFunction[T]]
      }

    def tDigest[T](aggregation: LeveledAggregatedFunction[T]): LeveledAggregatedFunction[T] =
      aggregation match {
        case median: Median[_]       => median.copy(modifier = TDigest).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantile: Quantile[_]   => quantile.copy(modifier = TDigest).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantiles: Quantiles[_] => quantiles.copy(modifier = TDigest).asInstanceOf[LeveledAggregatedFunction[T]]
      }

    def timing[T](aggregation: LeveledAggregatedFunction[T]) =
      aggregation match {
        case median: Median[_]       => median.copy(modifier = Timing).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantile: Quantile[_]   => quantile.copy(modifier = Timing).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantiles: Quantiles[_] => quantiles.copy(modifier = Timing).asInstanceOf[LeveledAggregatedFunction[T]]
      }

    def deterministic[T: Numeric](determinator: TableColumn[T])(aggregation: LeveledAggregatedFunction[T]) =
      aggregation match {
        case median: Median[_] =>
          median.copy(modifier = Deterministic(determinator)).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantile: Quantile[_] =>
          quantile.copy(modifier = Deterministic(determinator)).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantiles: Quantiles[_] =>
          quantiles.copy(modifier = Deterministic(determinator)).asInstanceOf[LeveledAggregatedFunction[T]]
      }

    def weighted[T](weight: TableColumn[Int])(aggregation: LeveledAggregatedFunction[T]) =
      aggregation match {
        case median: Median[_] =>
          median.copy(modifier = extractWeighted(median.modifier, weight)).asInstanceOf[LeveledAggregatedFunction[T]]
        case quantile: Quantile[_] =>
          quantile
            .copy(modifier = extractWeighted(quantile.modifier, weight))
            .asInstanceOf[LeveledAggregatedFunction[T]]
        case quantiles: Quantiles[_] =>
          quantiles
            .copy(modifier = extractWeighted(quantiles.modifier, weight))
            .asInstanceOf[LeveledAggregatedFunction[T]]
      }

    private def extractWeighted(modifier: LevelModifier, weight: TableColumn[Int]) =
      modifier match {
        case Timing => TimingWeighted(weight)
        case Exact  => ExactWeighted(weight)
        case other =>
          throw new IllegalArgumentException(
            s"Cannot use modifier $other for weighted leveled (median, quantile, quantiles)"
          )
      }

  }

}

case class Min[V](tableColumn: TableColumn[V]) extends AggregateFunction[V](tableColumn)

case class Max[V](tableColumn: TableColumn[V]) extends AggregateFunction[V](tableColumn)

case class TimeSeries(tableColumn: TableColumn[Long],
                      interval: MultiInterval,
                      dateColumn: Option[TableColumn[DateTime]])
    extends AggregateFunction[Long](tableColumn)
