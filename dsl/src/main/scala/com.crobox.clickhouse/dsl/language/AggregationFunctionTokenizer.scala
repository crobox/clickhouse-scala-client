package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait AggregationFunctionTokenizer { this: ClickhouseTokenizerModule =>

  def tokenizeAggregateFunction(agg: AggregateFunction[_])(implicit ctx: TokenizeContext): String =
    agg match {
      case nested: CombinedAggregatedFunction[_, _] =>
        val tokenizedCombinators = collectCombinators(nested).map(tokenizeCombinator)
        val combinators          = tokenizedCombinators.map(_._1).mkString("")
        val combinatorsValues    = tokenizedCombinators.flatMap(_._2).mkString(",")
        val (function, values)   = tokenizeInnerAggregatedFunction(extractTarget(nested))
        val separator            = if (values.isEmpty || combinatorsValues.isEmpty) "" else ","
        s"$function$combinators($values$separator$combinatorsValues)"
      case timeSeries: TimeSeries => tokenizeTimeSeries(timeSeries)
      case aggregated: AggregateFunction[_] =>
        val (function, values) = tokenizeInnerAggregatedFunction(aggregated)
        s"$function($values)"
    }

  def collectCombinators(function: AggregateFunction[_]): Seq[Combinator[_, _]] =
    function match {
      case CombinedAggregatedFunction(combinator, aggregated) => collectCombinators(aggregated) :+ combinator
      case _                                                  => Seq()
    }

  def extractTarget(function: AggregateFunction[_]): AggregateFunction[_] =
    function match {
      case CombinedAggregatedFunction(_, aggregated) => extractTarget(aggregated)
      case value                                     => value
    }

  private def tokenizeInnerAggregatedFunction(
      agg: AggregateFunction[_]
  )(implicit ctx: TokenizeContext): (String, String) =
    agg match {
      case Avg(column)   => ("avg", tokenizeColumn(column))
      case Count(column) => ("count", tokenizeColumn(column.getOrElse(EmptyColumn)))
      case Median(column, level, modifier) =>
        val (modifierName, modifierValue) = tokenizeLevelModifier(modifier)
        (s"median$modifierName", s"$level)(${tokenizeColumn(column)}${modifierValue.map("," + _).getOrElse("")}")
      case Quantile(column, level, modifier) =>
        val (modifierName, modifierValue) = tokenizeLevelModifier(modifier)
        (s"quantile$modifierName", s"$level)(${tokenizeColumn(column)}${modifierValue.map("," + _).getOrElse("")})")
      case Quantiles(column, levels, modifier) =>
        val (modifierName, modifierValue) = tokenizeLevelModifier(modifier)
        (s"quantiles$modifierName",
         s"${levels.mkString(",")})(${tokenizeColumn(column)}${modifierValue.map("," + _).getOrElse("")}")
      case Uniq(columns, modifier) =>
        (s"uniq${tokenizeUniqModifier(modifier)}", columns.map(tokenizeColumn).mkString(","))
      case Sum(column, modifier)       => (s"sum${tokenizeSumModifier(modifier)}", tokenizeColumn(column))
      case SumMap(key, value)          => (s"sumMap", tokenizeColumns(Seq(key, value)))
      case AnyResult(column, modifier) => (s"any${tokenizeAnyModifier(modifier)}", tokenizeColumn(column))
      case Min(tableColumn)            => ("min", tokenizeColumn(tableColumn))
      case Max(tableColumn)            => ("max", tokenizeColumn(tableColumn))
      case GroupUniqArray(tableColumn) => ("groupUniqArray", tokenizeColumn(tableColumn))
      case GroupArray(tableColumn, maxValues) =>
        ("groupArray", s"${maxValues.map(_.toString + ")(").getOrElse("")}${tokenizeColumn(tableColumn)}")
      case f: AggregateFunction[_] =>
        throw new IllegalArgumentException(s"Cannot use $f aggregated function with combinator")
    }

  def tokenizeLevelModifier(level: LevelModifier)(implicit ctx: TokenizeContext): (String, Option[String]) =
    level match {
      case LevelModifier.Simple                      => ("", None)
      case LevelModifier.Deterministic(determinator) => ("Deterministic", Some(tokenizeColumn(determinator)))
      case LevelModifier.Timing                      => ("Timing", None)
      case LevelModifier.TimingWeighted(weight)      => ("TimingWeighted", Some(tokenizeColumn(weight)))
      case LevelModifier.Exact                       => ("Exact", None)
      case LevelModifier.ExactWeighted(weight)       => ("ExactWeighted", Some(tokenizeColumn(weight)))
      case LevelModifier.TDigest                     => ("TDigest", None)
    }

  def tokenizeUniqModifier(modifier: UniqModifier): String =
    modifier match {
      case UniqModifier.Simple   => ""
      case UniqModifier.Combined => "Combined"
      case UniqModifier.Exact    => "Exact"
      case UniqModifier.HLL12    => "HLL12"
    }

  def tokenizeSumModifier(modifier: SumModifier): String =
    modifier match {
      case SumModifier.Simple       => ""
      case SumModifier.WithOverflow => "WithOverflow"
      case SumModifier.Map          => "Map"
    }

  def tokenizeAnyModifier(modifier: AnyModifier): String =
    modifier match {
      case AnyModifier.Simple => ""
      case AnyModifier.Heavy  => "Heavy"
      case AnyModifier.Last   => "Last"
    }

  private def tokenizeCombinator(
      combinator: Combinator[_, _]
  )(implicit ctx: TokenizeContext): (String, Option[String]) =
    combinator match {
      case Combinator.If(condition)     => ("If", Some(tokenizeColumn(condition)))
      case Combinator.CombinatorArray() => ("Array", None)
      case Combinator.ArrayForEach()    => ("ForEach", None)
      case Combinator.State()           => ("State", None)
      case Combinator.Merge()           => ("Merge", None)
    }
}
