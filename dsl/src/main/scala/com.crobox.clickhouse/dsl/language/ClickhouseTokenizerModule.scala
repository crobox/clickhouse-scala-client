package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl
import com.crobox.clickhouse.dsl.AggregateFunction._
import com.crobox.clickhouse.dsl.AnyResult.AnyModifier
import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.Leveled.LevelModifier
import com.crobox.clickhouse.dsl.StringFunctions.{ConcatString, SplitString}
import com.crobox.clickhouse.dsl.Sum.SumModifier
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.Uniq.UniqModifier
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.time.TimeUnit.{apply => _, _}
import com.crobox.clickhouse.time.{MultiDuration, SimpleDuration}
import com.dongxiguo.fastring.Fastring.Implicits._
import com.google.common.base.Strings
import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

trait ClickhouseTokenizerModule extends TokenizerModule {
  private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def toSql(query: InternalQuery,
                     formatting: Option[String] = Some("JSON"))(implicit database: Database): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = (toRawSql(query) + formatSql)
    logger.debug(fast"Generated sql [$sql]")
    sql
  }

  private def toRawSql(query: InternalQuery)(implicit database: Database): String =
    //    require(query != null) because parallel query is null
    query match {
      case InternalQuery(select, from, asFinal, where, groupBy, having, join, orderBy, limit, union) =>
        val tokenizedFrom = tokenizeFrom(from)
        fast"""
           |${tokenizeSelect(select)}
           | ${if (tokenizedFrom.isEmpty) "" else s"FROM $tokenizedFrom"}
           | ${tokenizeFinal(asFinal)}
           | ${tokenizeJoin(join)}
           | ${tokenizeFiltering(where, "WHERE")}
           | ${tokenizeGroupBy(groupBy)}
           | ${tokenizeFiltering(having, "HAVING")}
           | ${tokenizeOrderBy(orderBy)}
           | ${tokenizeLimit(limit)}
           |${tokenizeUnionAll(union)}""".toString.trim.stripMargin.replaceAll("\n", "").replaceAll("\r", "").trim().replaceAll(" +", " ")
    }

  private def tokenizeUnionAll(unions: Seq[OperationalQuery])(implicit database: Database) =
    if (unions.nonEmpty) {
      unions.map(q => fast"""UNION ALL
               | ${toRawSql(q.internalQuery)}""".toString.stripMargin).mkString
    } else {
      ""
    }

  private def tokenizeSelect(select: Option[SelectQuery])(implicit database: Database) =
    select match {
      case Some(s) => fast"SELECT ${s.modifier} ${tokenizeColumns(s.columns)}"
      case _       => ""
    }

  private def tokenizeFrom(from: Option[FromQuery])(implicit database: Database): String = {
    require(from != null)

    from match {
      case Some(fromClause: InnerFromQuery) =>
        fast"(${toRawSql(fromClause.innerQuery.internalQuery)})"
      case Some(TableFromQuery(table: Table, None)) =>
        fast"$database.${table.name}"
      case Some(TableFromQuery(table: Table, Some(altDb))) =>
        fast"$altDb.${table.name}"
      case _ => ""
    }
  }

  private def tokenizeFinal(asFinal: Boolean): String = if (asFinal) "FINAL" else ""

  protected def tokenizeColumn(column: AnyTableColumn)(implicit database: Database): String = {
    require(column != null)
    column match {
      case AliasedColumn(original, alias) =>
        val originalColumnToken = tokenizeColumn(original)
        if (Strings.isNullOrEmpty(originalColumnToken)) alias else fast"$originalColumnToken AS $alias"
      case tuple: TupleColumn[_]         => fast"(${tuple.elements.map(tokenizeColumn).mkString(",")})"
      case col: ExpressionColumn[_]      => tokenizeExpressionColumn(col)
      case col: Comparison               => tokenizeCondition(col)
      case regularColumn: AnyTableColumn => regularColumn.name
    }
  }

  private def tokenizeExpressionColumn(col: ExpressionColumn[_])(implicit database: Database): String =
    col match {
      case ar: ArithmeticFunction[_] => tokenizeArithmeticFunction(ar)
      case agg: AggregateFunction[_] => tokenizeAggregateFunction(agg)
      case ArrayJoin(tableColumn)    => fast"arrayJoin(${tokenizeColumn(tableColumn)})"
      case SplitString(tableColumn, separator) =>
        if (separator.length == 1) {
          fast"splitByChar(${StringQueryValue(separator)},${tokenizeColumn(tableColumn)})"
        } else {
          fast"splitByString(${StringQueryValue(separator)},${tokenizeColumn(tableColumn)})"
        }
      case ConcatString(tableColumn, separator) =>
        fast"arrayStringConcat(${tokenizeColumn(tableColumn)},${StringQueryValue(separator)})"
      case All()                        => "*"
      case col: TypeCastColumn[_]       => tokenizeTypeCastColumn(col)
      case LowerCaseColumn(tableColumn) => fast"lowerUTF8(${tokenizeColumn(tableColumn)})"
      case Conditional(cases, default) =>
        fast"CASE ${cases
          .map(ccase => fast"WHEN ${tokenizeCondition(ccase.condition)} THEN ${tokenizeColumn(ccase.column)}")
          .mkString(" ")} ELSE ${tokenizeColumn(default)} END"
      case c: Const[_] => c.parsed
      case QueryColumn(query) => s"(${toRawSql(query.internalQuery)})"
    }

  //FIXME: Temporary until changes from voc update feature/voc-typed-magnets
  private def tokenizeArithmeticFunction(col: ArithmeticFunction[_])(implicit database: Database): String = col match {
    case Multiply(left: AnyTableColumn, right: AnyTableColumn) =>
      fast"multiply(${tokenizeColumn(left)},${tokenizeColumn(right)})"
    case Divide(left: AnyTableColumn, right: AnyTableColumn) =>
      fast"divide(${tokenizeColumn(left)},${tokenizeColumn(right)})"
    case Plus(left: AnyTableColumn, right: AnyTableColumn) =>
      fast"plus(${tokenizeColumn(left)},${tokenizeColumn(right)})"
    case Minus(left: AnyTableColumn, right: AnyTableColumn) =>
      fast"minus(${tokenizeColumn(left)},${tokenizeColumn(right)})"
  }

  private def tokenizeTypeCastColumn(col: TypeCastColumn[_])(implicit database: Database): String = {
    def tknz(orZero: Boolean): String =
      if (orZero) "OrZero" else ""

    col match {
      case UInt8(tableColumn, orZero)   => fast"toUInt8${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case UInt16(tableColumn, orZero)  => fast"toUInt16${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case UInt32(tableColumn, orZero)  => fast"toUInt32${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case UInt64(tableColumn, orZero)  => fast"toUInt64${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int8(tableColumn, orZero)    => fast"toInt8${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int16(tableColumn, orZero)   => fast"toInt16${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int32(tableColumn, orZero)   => fast"toInt32${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Int64(tableColumn, orZero)   => fast"toInt64${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Float32(tableColumn, orZero) => fast"toFloat32${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case Float64(tableColumn, orZero) => fast"toFloat64${tknz(orZero)}(${tokenizeColumn(tableColumn)})"
      case DateRep(tableColumn)         => fast"toDate(${tokenizeColumn(tableColumn)})"
      case DateTimeRep(tableColumn)     => fast"toDateTime(${tokenizeColumn(tableColumn)})"

      case StringRep(tableColumn)       => fast"toString(${tokenizeColumn(tableColumn)})"
      case FixedString(tableColumn, n)  => fast"toFixedString(${tokenizeColumn(tableColumn)},$n)"
      case StringCutToZero(tableColumn) => fast"toStringCutToZero(${tokenizeColumn(tableColumn)})"

      case Reinterpret(typeCastColumn) => "reinterpretAs" + tokenizeTypeCastColumn(typeCastColumn).substring(2)

      case Cast(tableColumn, simpleColumnType) => fast"cast(${tokenizeColumn(tableColumn)} AS $simpleColumnType)"
    }
  }

  private def tokenizeAggregateFunction(agg: AggregateFunction[_])(implicit database: Database): String =
    agg match {
      case nested: CombinedAggregatedFunction[_, _] =>
        val tokenizedCombinators = collectCombinators(nested).map(tokenizeCombinator)
        val combinators          = tokenizedCombinators.map(_._1).mkString("")
        val combinatorsValues    = tokenizedCombinators.flatMap(_._2).mkString(",")
        val (function, values)   = tokenizeInnerAggregatedFunction(extractTarget(nested))
        val separator            = if (Strings.isNullOrEmpty(values) || Strings.isNullOrEmpty(combinatorsValues)) "" else ","
        fast"$function$combinators($values$separator$combinatorsValues)"
      case timeSeries: TimeSeries => tokenizeTimeSeries(timeSeries)
      case aggregated: AggregateFunction[_] =>
        val (function, values) = tokenizeInnerAggregatedFunction(aggregated)
        fast"$function($values)"
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

  private def tokenizeInnerAggregatedFunction(agg: AggregateFunction[_])(implicit database: Database): (String, String) =
    agg match {
      case Avg(column)   => ("avg", tokenizeColumn(column))
      case Count(column) => ("count", tokenizeColumn(column.getOrElse(EmptyColumn())))
      case Median(column, level, modifier) =>
        val (modifierName, modifierValue) = tokenizeLevelModifier(modifier)
        (fast"median$modifierName", fast"$level)(${tokenizeColumn(column)}${modifierValue.map("," + _).getOrElse("")}")
      case Quantile(column, level, modifier) =>
        val (modifierName, modifierValue) = tokenizeLevelModifier(modifier)
        (fast"quantile$modifierName",
         fast"$level)(${tokenizeColumn(column)}${modifierValue.map("," + _).getOrElse("")})")
      case Quantiles(column, levels, modifier) =>
        val (modifierName, modifierValue) = tokenizeLevelModifier(modifier)
        (fast"quantiles$modifierName",
         fast"${levels.mkString(",")})(${tokenizeColumn(column)}${modifierValue.map("," + _).getOrElse("")}")
      case Uniq(column, modifier)      => (s"uniq${tokenizeUniqModifier(modifier)}", tokenizeColumn(column))
      case Sum(column, modifier)       => (s"sum${tokenizeSumModifier(modifier)}", tokenizeColumn(column))
      case SumMap(key, value)          => (s"sumMap", tokenizeColumns(Seq(key, value)))
      case AnyResult(column, modifier) => (s"any${tokenizeAnyModifier(modifier)}", tokenizeColumn(column))
      case Min(tableColumn)            => ("min", tokenizeColumn(tableColumn))
      case Max(tableColumn)            => ("max", tokenizeColumn(tableColumn))
      case GroupUniqArray(tableColumn) => ("groupUniqArray", tokenizeColumn(tableColumn))
      case GroupArray(tableColumn, maxValues) =>
        ("groupArray", fast"${maxValues.map(_.toString + ")(").getOrElse("")}${tokenizeColumn(tableColumn)}")
      case f: AggregateFunction[_] =>
        throw new IllegalArgumentException(s"Cannot use $f aggregated function with combinator")
    }

  def tokenizeLevelModifier(level: LevelModifier)(implicit database: Database): (String, Option[String]) =
    level match {
      case Leveled.Simple                      => ("", None)
      case Leveled.Deterministic(determinator) => ("Deterministic", Some(tokenizeColumn(determinator)))
      case Leveled.Timing                      => ("Timing", None)
      case Leveled.TimingWeighted(weight)      => ("TimingWeighted", Some(tokenizeColumn(weight)))
      case Leveled.Exact                       => ("Exact", None)
      case Leveled.ExactWeighted(weight)       => ("ExactWeighted", Some(tokenizeColumn(weight)))
      case Leveled.TDigest                     => ("TDigest", None)
    }

  def tokenizeUniqModifier(modifier: UniqModifier): String =
    modifier match {
      case Uniq.Simple   => ""
      case Uniq.Combined => "Combined"
      case Uniq.Exact    => "Exact"
      case Uniq.HLL12    => "HLL12"

    }

  def tokenizeSumModifier(modifier: SumModifier): String =
    modifier match {
      case Sum.Simple       => ""
      case Sum.WithOverflow => "WithOverflow"
      case Sum.Map          => "Map"
    }

  def tokenizeAnyModifier(modifier: AnyModifier): String =
    modifier match {
      case AnyResult.Simple => ""
      case AnyResult.Heavy  => "Heavy"
      case AnyResult.Last   => "Last"
    }

  private def tokenizeCombinator(combinator: AggregateFunction.Combinator[_, _])(implicit database: Database): (String, Option[String]) =
    combinator match {
      case If(condition)     => ("If", Some(tokenizeCondition(condition)))
      case CombinatorArray() => ("Array", None)
      case ArrayForEach()    => ("ForEach", None)
      case State()           => ("State", None)
      case Merge()           => ("Merge", None)
    }

  private[language] def tokenizeTimeSeries(timeSeries: TimeSeries)(implicit database: Database): String = {
    val column = tokenizeColumn(timeSeries.tableColumn)
    tokenizeDuration(timeSeries, column)
  }

  private def tokenizeDuration(timeSeries: TimeSeries, column: String) = {
    val interval = timeSeries.interval
    val dateZone = determineZoneId(interval.rawStart)

    def toNthMonth(nth: Int) = {
      val startOfMonth = fast"toStartOfMonth(toDateTime($column / 1000), '$dateZone')"
      if (nth == 1) {
        fast"toDateTime($startOfMonth, '$dateZone')"
      } else {
        fast"toDateTime(addMonths($startOfMonth, 0 - (toRelativeMonthNum(toDateTime($column / 1000), '$dateZone') % $nth)), '$dateZone')"
      }
    }

    interval.duration match {
      case MultiDuration(1, Year) =>
        fast"toDateTime(toStartOfYear(toDateTime($column / 1000), '$dateZone'), '$dateZone')"
      case MultiDuration(1, Week) =>
        fast"toDateTime(toMonday(toDateTime($column / 1000), '$dateZone'), '$dateZone')"
      case MultiDuration(1, Day) =>
        fast"toStartOfDay(toDateTime($column / 1000), '$dateZone')"
      case MultiDuration(1, Hour) =>
        fast"toStartOfHour(toDateTime($column / 1000), '$dateZone')"
      case MultiDuration(1, Minute) =>
        fast"toStartOfMinute(toDateTime($column / 1000), '$dateZone')"
      case MultiDuration(1, Second) =>
        fast"toDateTime($column / 1000, '$dateZone')"
      case MultiDuration(nth, Year) =>
        fast"toDateTime(addYears(toStartOfYear(toDateTime($column / 1000), '$dateZone'), 0 - (toYear(toDateTime($column / 1000), '$dateZone') % $nth)), '$dateZone')"
      case MultiDuration(nth, Quarter) =>
        toNthMonth(nth * 3)
      case MultiDuration(nth, Month) =>
        toNthMonth(nth)
      case MultiDuration(nth, Week) =>
        fast"toDateTime(addWeeks(toMonday(toDateTime($column / 1000), '$dateZone'), 0 - ((toRelativeWeekNum(toDateTime($column / 1000), '$dateZone') - 1) % $nth)), '$dateZone')"
      case MultiDuration(nth, Day) =>
        fast"addDays(toStartOfDay(toDateTime($column / 1000), '$dateZone'), 0 - (toRelativeDayNum(toDateTime($column / 1000), '$dateZone') % $nth), '$dateZone')"
      case MultiDuration(nth, Hour) =>
        fast"addHours(toStartOfHour(toDateTime($column / 1000), '$dateZone'), 0 - (toRelativeHourNum(toDateTime($column / 1000), '$dateZone') % $nth), '$dateZone')"
      case MultiDuration(nth, Minute) =>
        fast"addMinutes(toStartOfMinute(toDateTime($column / 1000), '$dateZone'), 0 - (toRelativeMinuteNum(toDateTime($column / 1000), '$dateZone') % $nth), '$dateZone')"
      case MultiDuration(nth, Second) =>
        fast"addSeconds(toDateTime($column / 1000, '$dateZone'), 0 - (toRelativeSecondNum(toDateTime($column / 1000), '$dateZone') % $nth), '$dateZone')"
      case SimpleDuration(Total) => fast"${interval.getStartMillis}"
    }
  }

  //TODO this is a fallback to find a similar timezone when the provided interval does not have a set timezone id. We should be able to disable this from the config and fail fast if we cannot determine the timezone for timeseries (probably default to failing)
  private def determineZoneId(start: DateTime) = {
    val provider = DateTimeZone.getProvider
    val zones    = provider.getAvailableIDs.asScala.map(provider.getZone)
    val zone     = start.getZone
    val targetZone = zones
      .find(_.getID == zone.getID)
      .orElse(
        zones.find(targetZone => targetZone.getOffset(start.getMillis) == start.getZone.getOffset(start.getMillis))
      )
      .getOrElse(throw new IllegalArgumentException(s"Could not determine the zone from source $zone"))
      .getID
    targetZone
  }
  //  Table joins are tokenized as select * because of https://github.com/yandex/ClickHouse/issues/635
  private def tokenizeJoin(option: Option[JoinQuery])(implicit database: Database): String =
    option match {
      case None =>
        ""
      case Some(JoinQuery(joinType, tableJoin: TableFromQuery[_], usingCols)) =>
        fast"${tokenizeJoinType(joinType)} (SELECT * FROM ${tokenizeFrom(Some(tableJoin))}) USING ${tokenizeColumns(usingCols)}"
      case Some(JoinQuery(joinType, innerJoin: InnerFromQuery, usingCols)) =>
        fast"${tokenizeJoinType(joinType)} ${tokenizeFrom(Some(innerJoin))} USING ${tokenizeColumns(usingCols)}"
    }

  private def tokenizeColumns(columns: Set[AnyTableColumn])(implicit database: Database): String =
    columns.map(tokenizeColumn).mkString(", ")

  private def tokenizeColumns(columns: Seq[AnyTableColumn])(implicit database: Database): String =
    columns
      .filterNot {
        case _: EmptyColumn => true
        case _              => false
      }
      .map(tokenizeColumn)
      .mkString(", ")

  private def tokenizeJoinType(joinType: JoinQuery.JoinType): String =
    joinType match {
      case AnyInnerJoin => "ANY INNER JOIN"
      case AnyLeftJoin  => "ANY LEFT JOIN"
      case AnyRightJoin => "ANY RIGHT JOIN"
      case AllLeftJoin  => "ALL LEFT JOIN"
      case AllRightJoin => "ALL RIGHT JOIN"
      case AllInnerJoin => "ALL INNER JOIN"
    }

  private def tokenizeFiltering(maybeCondition: Option[dsl.Comparison], keyword: String)(implicit database: Database): String =
    maybeCondition match {
      case None            => ""
      case Some(condition) => fast"$keyword ${tokenizeCondition(condition)}"
    }

  protected def tokenizeCondition(condition: Comparison)(implicit database: Database): String =
    condition match {
      case _: NoOpComparison                             => ""
      case ColRefColumnComparison(left, operator, right) => fast"${aliasOrName(left)} $operator ${aliasOrName(right)}"
      case vcc @ ValueColumnComparison(left, operator, right) =>
        fast"${aliasOrName(left)} $operator ${vcc.queryValueEvidence(right)}"
      case FunctionColumnComparison(function, column) => fast"$function(${aliasOrName(column)})"
      case HigherOrderFunction(function, comparisonColumn, comparison, column) =>
        fast"$function(${tokenizeColumn(comparisonColumn)} -> ${tokenizeCondition(comparison)}, ${tokenizeColumn(column)})"
      case ChainableColumnCondition(_: NoOpComparison, _, _: NoOpComparison) => ""
      case ChainableColumnCondition(left, _, _: NoOpComparison)              => fast"${tokenizeCondition(left)}"
      case ChainableColumnCondition(_: NoOpComparison, _, right)             => fast"${tokenizeCondition(right)}"
      case ChainableColumnCondition(left, operator, right) if operator == "OR" =>
        fast"(${tokenizeCondition(left)} $operator ${tokenizeCondition(right)})"
      case ChainableColumnCondition(left, operator, right) =>
        fast"${tokenizeCondition(left)} $operator ${tokenizeCondition(right)}"
    }

  private def tokenizeGroupBy(groupBy: Seq[AnyTableColumn])(implicit database: Database): String =
    groupBy.toList match {
      case Nil | null => ""
      case _          => fast"GROUP BY ${tokenizeColumnsAliased(groupBy)}"
    }

  private def tokenizeOrderBy(orderBy: Seq[(AnyTableColumn, OrderingDirection)])(implicit database: Database): String =
    orderBy.toList match {
      case Nil | null => ""
      case _          => fast"ORDER BY ${tokenizeTuplesAliased(orderBy)}"
    }

  private def tokenizeLimit(limit: Option[Limit]): String =
    limit match {
      case None                      => ""
      case Some(Limit(size, offset)) => fast" LIMIT $offset, $size"
    }

  private def tokenizeColumnsAliased(columns: Seq[AnyTableColumn])(implicit database: Database): String =
    columns.map(aliasOrName).mkString(", ")

  private def tokenizeTuplesAliased(columns: Seq[(AnyTableColumn, OrderingDirection)])(implicit database: Database): String =
    columns
      .map {
        case (column, dir) =>
          aliasOrName(column) + " " + direction(dir)
      }
      .mkString(", ")

  private def aliasOrName(column: AnyTableColumn)(implicit database: Database) =
    column match {
      case AliasedColumn(_, alias)       => alias
      case QueryColumn(query) => s"(${toRawSql(query.internalQuery)})"
      case regularColumn: AnyTableColumn => regularColumn.name
    }

  private def direction(dir: OrderingDirection): String =
    dir match {
      case ASC  => "ASC"
      case DESC => "DESC"
    }

}
