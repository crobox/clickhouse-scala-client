package com.crobox.clickhouse.dsl.column


import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.time.MultiInterval

trait AggregationFunctions
   { self: Magnets with SumFunctions
     with AnyResultFunctions
     with UniqFunctions
     with Leveled
     with AggregationFunctionsCombiners =>

  //TODO: Magnetize?
  // Aggregate functions are a whole different beast, they are intercompatible and type passing in a different way then
  // what most other functions work like

  //https://clickhouse.yandex/docs/en/agg_functions/reference

  abstract class AggregateFunction[V](targetColumn: AnyTableColumn) extends ExpressionColumn[V](targetColumn)

  case class Count(column: Option[AnyTableColumn] = None) extends AggregateFunction[Long](column.getOrElse(EmptyColumn()))

  case class Avg[V](tableColumn: TableColumn[V]) extends AggregateFunction[Double](tableColumn)

  case class GroupUniqArray[V](tableColumn: TableColumn[V]) extends AggregateFunction[Seq[V]](tableColumn)

  case class GroupArray[V](tableColumn: TableColumn[V], maxValues: Option[Long])
    extends AggregateFunction[Seq[V]](tableColumn)

  case class Min[V](tableColumn: TableColumn[V]) extends AggregateFunction[V](tableColumn)

  case class Max[V](tableColumn: TableColumn[V]) extends AggregateFunction[V](tableColumn)

  case class TimeSeries(tableColumn: TableColumn[Long],
    interval: MultiInterval)
    extends AggregateFunction[Long](tableColumn)

    def count() =
      Count()
  
    def count(column: TableColumn[_]) =
      Count(Option(column))
  
    def average[T](tableColumn: TableColumn[T]) =
      Avg(tableColumn)
  
    def min[V](tableColumn: TableColumn[V]) =
      Min(tableColumn)
  
    def max[V](tableColumn: TableColumn[V]) =
      Max(tableColumn)

  /**
    * This function will push back the timestamp represented by tableColumn to the start of this interval,
    * this happens deterministically.
    *
    * Meaning that as long as the duration is the same, your groups will be in the same from/to timestamps
    *
    * This is useful for aggregating results by periods of time (group by month, 2 months, days, etc.)
    *
    * @param tableColumn
    * @param interval
    * @return
    */
  def timeSeries(tableColumn: TableColumn[Long], interval: MultiInterval) =
    TimeSeries(tableColumn, interval)

    def groupUniqArray[V](tableColumn: TableColumn[V]) =
      GroupUniqArray(tableColumn)

}

trait AggregationFunctionsCombiners { self: Magnets with AggregationFunctions =>
  
  case class CombinedAggregatedFunction[T <: TableColumn[_], Res](combinator: Combinator[T, Res],
    target: AggregateFunction[_])
    extends AggregateFunction[Res](EmptyColumn())
  
  sealed trait StateResult[V]

  sealed trait Combinator[T <: Column, Result]

  object Combinator {
    case class If[T <: Column, Res](condition: TableColumn[Boolean])    extends Combinator[T, Res]
    case class CombinatorArray[T <: TableColumn[Seq[V]], V]() extends Combinator[T, V]
    case class ArrayForEach[T <: TableColumn[Seq[_]], Res]()  extends Combinator[T, Seq[Res]]
    case class State[T <: Column, Res]()                      extends Combinator[T, StateResult[Res]]
    case class Merge[T <: TableColumn[_], Res]()              extends Combinator[T, Res]
  }

    def aggIf[T <: TableColumn[Res], Res](condition: TableColumn[Boolean])(aggregated: AggregateFunction[Res]): CombinedAggregatedFunction[T, Res] =
      CombinedAggregatedFunction(Combinator.If(condition), aggregated)

    def array[T <: TableColumn[Seq[Res]], Res](aggregated: AggregateFunction[Res]): CombinedAggregatedFunction[T, Res] =
      CombinedAggregatedFunction(Combinator.CombinatorArray[T, Res](), aggregated)

    /**
      * Having a column with type array, it aggregates all the results for that column by running the provided aggregation functions for each vertical slice of the array elements.
      * Therefore, for the query result:
      * \array_col|
      * |[x1, y1, z1, u1]
      * |[x2, y2, z2]
      * |[x3, y3, z3]
      *
      * if you run sumForEach(array_col) you will get an array result with the following entries: [sum(x1,x3,x3), sum(y1,y2,y3), sum(z1, z2, z3), sum(u1)]
      *
      */
    def forEach[V, T <: TableColumn[Seq[V]], Res](
      column: T
    )(forEachFunc: TableColumn[V] => AggregateFunction[Res]): AggregateFunction[Seq[Res]] =
      CombinedAggregatedFunction(Combinator.ArrayForEach(), forEachFunc(ref[V](column.name)))

    def state[T <: TableColumn[Res], Res](aggregated: AggregateFunction[Res]
    ): CombinedAggregatedFunction[T, StateResult[Res]] =
      CombinedAggregatedFunction(
        Combinator.State[T, Res](),
        aggregated.asInstanceOf[AggregateFunction[StateResult[Res]]])


    def merge[T <: TableColumn[StateResult[Res]], Res](aggregated: AggregateFunction[Res]): CombinedAggregatedFunction[T, Res] =
      CombinedAggregatedFunction(Combinator.Merge[T, Res](), aggregated)

}

trait UniqFunctions { self: Magnets with AggregationFunctions =>
  sealed trait UniqModifier

  case class Uniq(tableColumn: AnyTableColumn, modifier: UniqModifier = UniqModifier.Simple)
    extends AggregateFunction[Long](tableColumn)
  
  object UniqModifier {
    case object Simple   extends UniqModifier
    case object Combined extends UniqModifier
    case object HLL12    extends UniqModifier
    case object Exact    extends UniqModifier
  }

    def uniq(tableColumn: AnyTableColumn) =
      Uniq(tableColumn)
    def uniqCombined(tableColumn: AnyTableColumn): Uniq = Uniq(tableColumn, UniqModifier.Combined)
    def uniqExact(tableColumn: AnyTableColumn): Uniq    = Uniq(tableColumn, UniqModifier.Exact)
    def uniqHLL12(tableColumn: AnyTableColumn): Uniq    = Uniq(tableColumn, UniqModifier.HLL12)

}


trait AnyResultFunctions { self: Magnets with AggregationFunctions =>
  case class AnyResult[T](tableColumn: TableColumn[T], modifier: AnyModifier = AnyModifier.Simple)
    extends AggregateFunction[T](tableColumn)

  sealed trait AnyModifier
  object AnyModifier {
    case object Simple extends AnyModifier
    case object Heavy  extends AnyModifier
    case object Last   extends AnyModifier
  }

    def any[T](tableColumn: TableColumn[T]): AnyResult[T] =
      AnyResult(tableColumn)

    def anyHeavy[T](tableColumn: TableColumn[T]): AnyResult[T] =
      AnyResult(tableColumn, AnyModifier.Heavy)

    def anyLast[T](tableColumn: TableColumn[T]): AnyResult[T] =
      AnyResult(tableColumn, AnyModifier.Last)

}

trait SumFunctions { self: Magnets with AggregationFunctions =>
  case class Sum[T](tableColumn: TableColumn[T], modifier: SumModifier = SumModifier.Simple)
    extends AggregateFunction[Double](tableColumn)

  case class SumMap[T, V](key: TableColumn[Seq[T]], value: TableColumn[Seq[V]])
    extends AggregateFunction[(Seq[T], Seq[V])](key)
  
  sealed trait SumModifier
  
  object SumModifier {
    case object Simple       extends SumModifier
    case object WithOverflow extends SumModifier
    case object Map          extends SumModifier
  }

    def sum[T](tableColumn: TableColumn[T]) =
      Sum(tableColumn)

    def sumOverflown[T](tableColumn: TableColumn[T]) =
      Sum(tableColumn, SumModifier.WithOverflow)

    def sumMap[T, V](key: TableColumn[Seq[T]], value: TableColumn[Seq[V]]) =
      SumMap(key, value)

}


trait Leveled { self: Magnets with AggregationFunctions =>
  sealed abstract class LeveledAggregatedFunction[T](target: AnyTableColumn) extends AggregateFunction[T](target)
  
  sealed trait LevelModifier
  
  object LevelModifier {
    case object Simple                                                         extends LevelModifier
    case object Exact                                                          extends LevelModifier
    case object TDigest                                                        extends LevelModifier
    case class Deterministic[T](determinator: TableColumn[T])         extends LevelModifier
    /*Works for numbers. Intended for calculating quantiles of page loading time in milliseconds.*/
    case object Timing extends LevelModifier
    /*The result is calculated as if the x value were passed weight number of times to the quantileTiming function.*/
    case class TimingWeighted(weight: TableColumn[Int]) extends LevelModifier
    case class ExactWeighted(weight: TableColumn[Int])  extends LevelModifier
  }
  


  /*Works for numbers, dates, and dates with times. Returns: for numbers – Float64; for dates – a date; for dates with times – a date with time.Works for numbers, dates, and dates with times. Returns: for numbers – Float64; for dates – a date; for dates with times – a date with time.*/
  case class Quantile[T](tableColumn: TableColumn[T], level: Float = 0.5F, modifier: LevelModifier = LevelModifier.Simple)
    extends LeveledAggregatedFunction[T](tableColumn) {
    require(level >= 0 && level <= 1)
  }
  case class Quantiles[T](tableColumn: TableColumn[T], levels: Seq[Float], modifier: LevelModifier = LevelModifier.Simple)
    extends LeveledAggregatedFunction[Seq[T]](tableColumn) {
    levels.foreach(level => require(level >= 0 && level <= 1))
  }
  case class Median[T](tableColumn: TableColumn[T], level: Float, modifier: LevelModifier = LevelModifier.Simple)
    extends LeveledAggregatedFunction[T](tableColumn) {
    require(level > 0 && level < 1)
  }


    def median[V](target: TableColumn[V], level: Float = 0.5F) = Median(target, level = level)

    def quantile[V](target: TableColumn[V], level: Float = 0.5F) = Quantile(target, level = level)

    def quantiles[V](target: TableColumn[V], levels: Float*) = Quantiles(target, levels)

    def medianExact[V](target: TableColumn[V], level: Float = 0.5F) = Median(target, level, LevelModifier.Exact)

    def quantileExact[V](target: TableColumn[V], level: Float = 0.5F) = Quantile(target, level, LevelModifier.Exact)

    def quantilesExact[V](target: TableColumn[V], levels: Float*) = Quantiles(target, levels, LevelModifier.Exact)

    def medianExactWeighted[V](target: TableColumn[V], weight: TableColumn[Int], level: Float = 0.5F) =
      Median(target, level, LevelModifier.ExactWeighted(weight))

    def quantileExactWeighted[V](target: TableColumn[V], weight: TableColumn[Int], level: Float = 0.5F) =
      Quantile(target, level, LevelModifier.ExactWeighted(weight))

    def quantilesExactWeighted[V](target: TableColumn[V], weight: TableColumn[Int], levels: Float*) =
      Quantiles(target, levels, LevelModifier.ExactWeighted(weight))

    def medianTDigest[V](target: TableColumn[V], level: Float = 0.5F) = Median(target, level, LevelModifier.TDigest)

    def quantileTDigest[V](target: TableColumn[V], level: Float = 0.5F) = Quantile(target, level, LevelModifier.TDigest)

    def quantilesTDigest[V](target: TableColumn[V], levels: Float*) = Quantiles(target, levels, LevelModifier.TDigest)

    def medianTiming[V](target: TableColumn[V], level: Float = 0.5F) = Median(target, level, LevelModifier.Timing)

    def quantileTiming[V](target: TableColumn[V], level: Float = 0.5F) = Quantile(target, level, LevelModifier.Timing)

    def quantilesTiming[V](target: TableColumn[V], levels: Float*) = Quantiles(target, levels, LevelModifier.Timing)

    def medianTimingWeighted[V](target: TableColumn[V], weight: TableColumn[Int], level: Float = 0.5F) =
      Median(target, level, LevelModifier.TimingWeighted(weight))

    def quantileTimingWeighted[V](target: TableColumn[V], weight: TableColumn[Int], level: Float = 0.5F) =
      Quantile(target, level, LevelModifier.TimingWeighted(weight))

    def quantilesTimingWeighted[V](target: TableColumn[V], weight: TableColumn[Int], levels: Float*) =
      Quantiles(target, levels, LevelModifier.TimingWeighted(weight))

    def medianDeterministic[V, T](target: TableColumn[V], determinator: TableColumn[T], level: Float = 0.5F) =
      Median(target, level, LevelModifier.Deterministic(determinator))

    def quantileDeterministic[V, T](target: TableColumn[V],
                                             determinator: TableColumn[T],
                                             level: Float = 0.5F) =
      Quantile(target, level, LevelModifier.Deterministic(determinator))

    def quantilesDeterministic[V, T](target: TableColumn[V], determinator: TableColumn[T], levels: Float*) =
      Quantiles(target, levels, LevelModifier.Deterministic(determinator))


}


