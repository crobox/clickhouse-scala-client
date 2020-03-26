package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.time.{MultiDuration, TimeUnit, TotalDuration}
import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

trait ClickhouseTokenizerModule
    extends TokenizerModule
    with AggregationFunctionTokenizer
    with ArithmeticFunctionTokenizer
    with ArrayFunctionTokenizer
    with BitFunctionTokenizer
    with ComparisonFunctionTokenizer
    with DateTimeFunctionTokenizer
    with DictionaryFunctionTokenizer
    with EncodingFunctionTokenizer
    with HashFunctionTokenizer
    with HigherOrderFunctionTokenizer
    with IPFunctionTokenizer
    with InFunctionTokenizer
    with JsonFunctionTokenizer
    with LogicalFunctionTokenizer
    with MathematicalFunctionTokenizer
    with MiscellaneousFunctionTokenizer
    with RandomFunctionTokenizer
    with RoundingFunctionTokenizer
    with SplitMergeFunctionTokenizer
    with StringFunctionTokenizer
    with StringSearchFunctionTokenizer
    with TypeCastFunctionTokenizer
    with URLFunctionTokenizer {

  private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  protected def tokenizeSeqCol[C <: TableColumn[_]](colSeq: Seq[C]): String = {
    val prefix = if (colSeq.isEmpty) "" else ", "
    prefix + colSeq.map(tokenizeColumn).mkString(", ")
  }

  override def toSql(query: InternalQuery, formatting: Option[String] = Some("JSON")): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = (toRawSql(query) + formatSql).trim().replaceAll(" +", " ")
    logger.debug(s"Generated sql [$sql]")
    sql
  }

  private[language] def toRawSql(query: InternalQuery): String =
    //    require(query != null) because parallel query is null
    query match {
      case InternalQuery(select, from, asFinal, prewhere, where, groupBy, having, join, orderBy, limit, union) =>
        s"""
           |${tokenizeSelect(select)}
           | ${tokenizeFrom(from)}
           | ${tokenizeFinal(asFinal)}
           | ${tokenizeJoin(join)}
           | ${tokenizeFiltering(prewhere, "PREWHERE")}
           | ${tokenizeFiltering(where, "WHERE")}
           | ${tokenizeGroupBy(groupBy)}
           | ${tokenizeFiltering(having, "HAVING")}
           | ${tokenizeOrderBy(orderBy)}
           | ${tokenizeLimit(limit)}
           | ${tokenizeUnionAll(union)}""".toString.trim.stripMargin.replaceAll("\n", "").replaceAll("\r", "")
    }

  private def tokenizeUnionAll(unions: Seq[OperationalQuery]) =
    if (unions.nonEmpty) {
      unions.map(q => s"UNION ALL ${toRawSql(q.internalQuery)}").mkString
    } else {
      ""
    }

  private def tokenizeSelect(select: Option[SelectQuery]) =
    select match {
      case Some(s) => s"SELECT ${s.modifier} ${tokenizeColumns(s.columns)}"
      case _       => ""
    }

  private def tokenizeFrom(from: Option[FromQuery], withPrefix: Boolean = true) = {
    require(from != null)

    val prefix = if (withPrefix) "FROM" else ""
    from match {
      case Some(fromClause: InnerFromQuery) =>
        s"$prefix (${toRawSql(fromClause.innerQuery.internalQuery)})"
      case Some(TableFromQuery(table: Table)) =>
        s"$prefix ${table.quoted}"
      case _ => ""
    }
  }

  private def tokenizeFinal(asFinal: Boolean): String = if (asFinal) "FINAL" else ""

  protected def tokenizeColumn(column: Column): String = {
    require(column != null)
    column match {
      case EmptyColumn => ""
      case alias: AliasedColumn[_] =>
        val originalColumnToken = tokenizeColumn(alias.original)
        if (originalColumnToken.isEmpty) alias.quoted else s"$originalColumnToken AS ${alias.quoted}"
      case tuple: TupleColumn[_]    => s"(${tuple.elements.map(tokenizeColumn).mkString(",")})"
      case col: LogicalFunction     => s"(${tokenizeExpressionColumn(col)})"
      case col: ExpressionColumn[_] => tokenizeExpressionColumn(col)
      case col: Column              => col.quoted
    }
  }

  private def tokenizeExpressionColumn(inCol: ExpressionColumn[_]): String =
    inCol match {
      case agg: AggregateFunction[_]         => tokenizeAggregateFunction(agg)
      case col: ArithmeticFunctionCol[_]     => tokenizeArithmeticFunctionColumn(col)
      case col: ArithmeticFunctionOp[_]      => tokenizeArithmeticFunctionOperator(col)
      case col: ArrayFunction                => tokenizeArrayFunction(col)
      case col: BitFunction                  => tokenizeBitFunction(col)
      case col: ComparisonColumn             => tokenizeComparisonColumn(col)
      case col: DateTimeFunctionCol[_]       => tokenizeDateTimeColumn(col)
      case col: DateTimeConst[_]             => tokenizeDateTimeConst(col)
      case col: DictionaryFuncColumn[_]      => tokenizeDictionaryFunction(col)
      case col: EncodingFunction[_]          => tokenizeEncodingFunction(col)
      case col: HashFunction                 => tokenizeHashFunction(col)
      case col: HigherOrderFunction[_, _, _] => tokenizeHigherOrderFunction(col)
      case col: IPFunction[_]                => tokenizeIPFunction(col)
      case col: InFunction                   => tokenizeInFunction(col)
      case col: JsonFunction[_]              => tokenizeJsonFunction(col)
      case col: LogicalFunction              => tokenizeLogicalFunction(col)
      case col: MathFuncColumn               => tokenizeMathematicalFunction(col)
      case col: MiscellaneousFunction        => tokenizeMiscellaneousFunction(col)
      case col: RandomFunction               => tokenizeRandomFunction(col)
      case col: RoundingFunction             => tokenizeRoundingFunction(col)
      case col: SplitMergeFunction[_]        => tokenizeSplitMergeFunction(col)
      case col: StringFunctionCol[_]         => tokenizeStringCol(col)
      case col: StringSearchFunc[_]          => tokenizeStringSearchFunction(col)
      case col: TypeCastColumn[_]            => tokenizeTypeCastColumn(col)
      case col: URLFunction[_]               => tokenizeURLFunction(col)
      case All()                             => "*"
      case RawColumn(rawSql)                 => rawSql
      case Conditional(cases, default) =>
        s"CASE ${cases
          .map(`case` => s"WHEN ${tokenizeColumn(`case`.condition)} THEN ${tokenizeColumn(`case`.result)}")
          .mkString(" ")} ELSE ${tokenizeColumn(default)} END"
      case c: Const[_] => c.parsed
      case a @ _ =>
        throw new NotImplementedError(
          a.getClass.getCanonicalName + " with superclass " + a.getClass.getSuperclass.getCanonicalName + " could not be matched."
        )
    }

  private[language] def tokenizeTimeSeries(timeSeries: TimeSeries): String = {
    val column = tokenizeColumn(timeSeries.tableColumn)
    tokenizeDuration(timeSeries, column)
  }

  private def tokenizeDuration(timeSeries: TimeSeries, column: String): String = {
    val interval = timeSeries.interval
    val dateZone = determineZoneId(interval.rawStart)

    def convert(fn: String): String = s"$fn(toDateTime($column / 1000), '$dateZone')"

    def toDateTime(inner: String): String = s"toDateTime($inner, '$dateZone')"

    interval.duration match {
      case TotalDuration                      => s"${interval.getStartMillis}"
      case MultiDuration(1, TimeUnit.Year)    => toDateTime(convert("toStartOfYear"))
      case MultiDuration(1, TimeUnit.Quarter) => toDateTime(convert("toStartOfQuarter"))
      case MultiDuration(1, TimeUnit.Month)   => toDateTime(convert("toStartOfMonth"))
      case MultiDuration(1, TimeUnit.Week)    => toDateTime(convert("toMonday"))
      case MultiDuration(1, TimeUnit.Day)     => convert("toStartOfDay")
      case MultiDuration(1, TimeUnit.Hour)    => convert("toStartOfHour")
      case MultiDuration(1, TimeUnit.Minute)  => convert("toStartOfMinute")
      case MultiDuration(1, TimeUnit.Second)  => toDateTime(s"$column / 1000")
      case MultiDuration(nth, TimeUnit.Year) =>
        toDateTime(s"subtractYears(${convert("toStartOfYear")}, ${convert("toRelativeYearNum")} % $nth)")
      case MultiDuration(nth, TimeUnit.Quarter) =>
        toDateTime(s"subtractMonths(${convert("toStartOfQuarter")}, (${convert("toRelativeQuarterNum")} % $nth) * 3)")
      case MultiDuration(nth, TimeUnit.Month) =>
        toDateTime(s"subtractMonths(${convert("toStartOfMonth")}, ${convert("toRelativeMonthNum")} % $nth)")
      case MultiDuration(nth, TimeUnit.Week) =>
        toDateTime(s"subtractWeeks(${convert("toMonday")}, (${convert("toRelativeWeekNum")} - 1) % $nth)")
      case MultiDuration(nth, TimeUnit.Day) =>
        s"subtractDays(${convert("toStartOfDay")}, ${convert("toRelativeDayNum")} % $nth, '$dateZone')"
      case MultiDuration(nth, TimeUnit.Hour) =>
        s"subtractHours(${convert("toStartOfHour")}, ${convert("toRelativeHourNum")} % $nth, '$dateZone')"
      case MultiDuration(nth, TimeUnit.Minute) =>
        s"subtractMinutes(${convert("toStartOfMinute")}, ${convert("toRelativeMinuteNum")} % $nth, '$dateZone')"
      case MultiDuration(nth, TimeUnit.Second) =>
        s"subtractSeconds(${toDateTime(s"$column / 1000")}, ${convert("toRelativeSecondNum")} % $nth, '$dateZone')"
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
        zones.find(
          targetZone =>
            targetZone.getID.startsWith("Etc/") &&
            targetZone.getOffset(start.getMillis) == start.getZone.getOffset(start.getMillis)
        )
      )
      .getOrElse(throw new IllegalArgumentException(s"Could not determine the zone from source $zone"))
      .getID
    targetZone
  }

  //  Table joins are tokenized as select * because of https://github.com/yandex/ClickHouse/issues/635
  private def tokenizeJoin(option: Option[JoinQuery]): String =
    option match {
      case None =>
        ""
      case Some(JoinQuery(joinType, tableJoin: TableFromQuery[_], usingCols, global)) =>
        s"${isGlobal(global)}${tokenizeJoinType(joinType)} (SELECT * ${tokenizeFrom(Some(tableJoin))}) USING ${tokenizeColumns(usingCols)}"
      case Some(JoinQuery(joinType, innerJoin: InnerFromQuery, usingCols, global)) =>
        s"${isGlobal(global)}${tokenizeJoinType(joinType)} ${tokenizeFrom(Some(innerJoin), false)} USING ${tokenizeColumns(usingCols)}"
    }

  private def isGlobal(global: Boolean): String = if (global) "GLOBAL " else ""

  private[language] def tokenizeColumns(columns: Seq[Column]): String =
    columns
      .filterNot {
        case EmptyColumn => true
        case _           => false
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

  private def tokenizeFiltering(maybeCondition: Option[TableColumn[Boolean]], keyword: String): String =
    maybeCondition match {
      case None            => ""
      case Some(condition) => s"$keyword ${tokenizeColumn(condition)}"
    }

  private def tokenizeGroupBy(groupBy: Option[GroupByQuery]): String = {
    val groupByColumns = groupBy match {
      case Some(GroupByQuery(usingColumns, _, _)) if usingColumns.nonEmpty =>
        Some(s"GROUP BY ${tokenizeColumnsAliased(usingColumns)}")
      case _ =>
        None
    }
    val groupByMode = groupBy match {
      case Some(GroupByQuery(_, Some(mode), _)) =>
        Some(mode match {
          case GroupByQuery.WithRollup => "WITH ROLLUP"
          case GroupByQuery.WithCube   => "WITH CUBE"
        })
      case _ => None
    }
    val groupByWithTotals = groupBy match {
      case Some(GroupByQuery(_, _, true)) => Some("WITH TOTALS")
      case _                              => None
    }
    (groupByColumns ++ groupByMode ++ groupByWithTotals).mkString(" ")
  }

  private def tokenizeOrderBy(orderBy: Seq[(Column, OrderingDirection)]): String =
    orderBy.toList match {
      case Nil | null => ""
      case _          => s"ORDER BY ${tokenizeTuplesAliased(orderBy)}"
    }

  private def tokenizeLimit(limit: Option[Limit]): String =
    limit match {
      case None                      => ""
      case Some(Limit(size, offset)) => s"LIMIT $offset, $size"
    }

  private def tokenizeColumnsAliased(columns: Seq[Column]): String =
    columns.map(aliasOrName).mkString(", ")

  private def tokenizeTuplesAliased(columns: Seq[(Column, OrderingDirection)]): String =
    columns
      .map {
        case (column, dir) =>
          aliasOrName(column) + " " + direction(dir)
      }
      .mkString(", ")

  private def aliasOrName(column: Column) =
    column match {
      case EmptyColumn             => ""
      case alias: AliasedColumn[_] => alias.quoted
      case col: Column             => col.quoted
    }

  private def direction(dir: OrderingDirection): String =
    dir match {
      case ASC  => "ASC"
      case DESC => "DESC"
    }

}
