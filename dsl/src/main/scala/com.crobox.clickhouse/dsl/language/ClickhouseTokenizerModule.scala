package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.time.{MultiDuration, SimpleDuration, TimeUnit}
import com.dongxiguo.fastring.Fastring.Implicits._
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

  protected def tokenizeSeqCol[C <: TableColumn[_]](colSeq: Seq[C])(implicit database: Database): String = {
    val prefix = if (colSeq.isEmpty) "" else ", "
    prefix + colSeq.map(tokenizeColumn).mkString(", ")
  }

  override def toSql(query: InternalQuery,
                     formatting: Option[String] = Some("JSON"))(implicit database: Database): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = (toRawSql(query) + formatSql).trim().replaceAll(" +", " ")
    logger.debug(fast"Generated sql [$sql]")
    sql
  }

  private[language] def toRawSql(query: InternalQuery)(implicit database: Database): String =
    //    require(query != null) because parallel query is null
    query match {
      case InternalQuery(select, from, asFinal, prewhere, where, groupBy, having, join, orderBy, limit, union) =>
        fast"""
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

  private def tokenizeUnionAll(unions : Seq[OperationalQuery])(implicit database: Database) = {
    if (unions.nonEmpty) {
      unions.map(q => fast"UNION ALL ${toRawSql(q.internalQuery)}").mkString
    } else {
      ""
    }
  }

  private def tokenizeSelect(select: Option[SelectQuery])(implicit database: Database) =
    select match {
      case Some(s) => fast"SELECT ${s.modifier} ${tokenizeColumns(s.columns)}"
      case _       => ""
    }

  private def tokenizeFrom(from: Option[FromQuery], withPrefix: Boolean = true)(implicit database: Database) = {
    require(from != null)

    val prefix = if (withPrefix) "FROM" else ""
    from match {
      case Some(fromClause: InnerFromQuery) =>
        fast"$prefix (${toRawSql(fromClause.innerQuery.internalQuery)})"
      case Some(TableFromQuery(table: Table, None)) =>
        fast"$prefix $database.${table.name}"
      case Some(TableFromQuery(table: Table, Some(altDb))) =>
        fast"$prefix $altDb.${table.name}"
      case _ => ""
    }
  }

  private def tokenizeFinal(asFinal: Boolean): String = if (asFinal) "FINAL" else ""

  protected def tokenizeColumn(column: AnyTableColumn)(implicit database: Database): String = {
    require(column != null)
    column match {
      case EmptyColumn => ""
      case AliasedColumn(original, alias) =>
        val originalColumnToken = tokenizeColumn(original)
        if (originalColumnToken.isEmpty) alias else fast"$originalColumnToken AS $alias"
      case tuple: TupleColumn[_]         => fast"(${tuple.elements.map(tokenizeColumn).mkString(",")})"
      case col: ExpressionColumn[_]      => tokenizeExpressionColumn(col)
      case regularColumn: AnyTableColumn => regularColumn.name
    }
  }

  private def tokenizeExpressionColumn(inCol: ExpressionColumn[_])(implicit database: Database): String =
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
      case Conditional(cases, default) =>
        fast"CASE ${cases
          .map(`case` => fast"WHEN ${tokenizeColumn(`case`.condition)} THEN ${tokenizeColumn(`case`.result)}")
          .mkString(" ")} ELSE ${tokenizeColumn(default)} END"
      case c: Const[_] => c.parsed
      case a@_ => throw new NotImplementedError(a.getClass.getCanonicalName + " with superclass " + a.getClass.getSuperclass.getCanonicalName + " could not be matched.")
    }

  private[language] def tokenizeTimeSeries(timeSeries: TimeSeries)(implicit database: Database): String = {
    val column = tokenizeColumn(timeSeries.tableColumn)
    tokenizeDuration(timeSeries, column)
  }

  private def tokenizeDuration(timeSeries: TimeSeries, column: String):String = {
    val interval = timeSeries.interval
    val dateZone = determineZoneId(interval.rawStart)

    def convert(fn: String): String = fast"$fn(toDateTime($column / 1000), '$dateZone')"

    def toDateTime(inner: String): String = fast"toDateTime($inner, '$dateZone')"

    interval.duration match {
      case SimpleDuration(TimeUnit.Total) => fast"${interval.getStartMillis}"
      case MultiDuration(1, TimeUnit.Year) => toDateTime(convert("toStartOfYear"))
      case MultiDuration(1, TimeUnit.Quarter) => toDateTime(convert("toStartOfQuarter"))
      case MultiDuration(1, TimeUnit.Month) => toDateTime(convert("toStartOfMonth"))
      case MultiDuration(1, TimeUnit.Week) => toDateTime(convert("toMonday"))
      case MultiDuration(1, TimeUnit.Day) => convert("toStartOfDay")
      case MultiDuration(1, TimeUnit.Hour) => convert("toStartOfHour")
      case MultiDuration(1, TimeUnit.Minute) => convert("toStartOfMinute")
      case MultiDuration(1, TimeUnit.Second) => toDateTime(fast"$column / 1000")
      case MultiDuration(nth, TimeUnit.Year) =>
        toDateTime(fast"subtractYears(${convert("toStartOfYear")}, ${convert("toRelativeYearNum")} % $nth)")
      case MultiDuration(nth, TimeUnit.Quarter) =>
        toDateTime(fast"subtractMonths(${convert("toStartOfQuarter")}, (${convert("toRelativeQuarterNum")} % $nth) * 3)")
      case MultiDuration(nth, TimeUnit.Month) =>
        toDateTime(fast"subtractMonths(${convert("toStartOfMonth")}, ${convert("toRelativeMonthNum")} % $nth)")
      case MultiDuration(nth, TimeUnit.Week) =>
        toDateTime(fast"subtractWeeks(${convert("toMonday")}, (${convert("toRelativeWeekNum")} - 1) % $nth)")
      case MultiDuration(nth, TimeUnit.Day) =>
        fast"subtractDays(${convert("toStartOfDay")}, ${convert("toRelativeDayNum")} % $nth, '$dateZone')"
      case MultiDuration(nth, TimeUnit.Hour) =>
        fast"subtractHours(${convert("toStartOfHour")}, ${convert("toRelativeHourNum")} % $nth, '$dateZone')"
      case MultiDuration(nth, TimeUnit.Minute) =>
        fast"subtractMinutes(${convert("toStartOfMinute")}, ${convert("toRelativeMinuteNum")} % $nth, '$dateZone')"
      case MultiDuration(nth, TimeUnit.Second) =>
        fast"subtractSeconds(${toDateTime(fast"$column / 1000")}, ${convert("toRelativeSecondNum")} % $nth, '$dateZone')"
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
        zones.find(targetZone => targetZone.getID.startsWith("Etc/") &&
          targetZone.getOffset(start.getMillis) == start.getZone.getOffset(start.getMillis))
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
        fast"${tokenizeJoinType(joinType)} (SELECT * ${tokenizeFrom(Some(tableJoin))}) USING ${tokenizeColumns(usingCols)}"
      case Some(JoinQuery(joinType, innerJoin: InnerFromQuery, usingCols)) =>
        fast"${tokenizeJoinType(joinType)} ${tokenizeFrom(Some(innerJoin),false)} USING ${tokenizeColumns(usingCols)}"
    }

  private[language] def tokenizeColumns(columns: Set[AnyTableColumn])(implicit database: Database): String =
    tokenizeColumns(columns.toSeq)

  private[language] def tokenizeColumns(columns: Seq[AnyTableColumn])(implicit database: Database): String =
    columns.filterNot(_.name == EmptyColumn.name).map(tokenizeColumn).mkString(", ")

  private def tokenizeJoinType(joinType: JoinQuery.JoinType): String =
    joinType match {
      case AnyInnerJoin => "ANY INNER JOIN"
      case AnyLeftJoin  => "ANY LEFT JOIN"
      case AnyRightJoin => "ANY RIGHT JOIN"
      case AllLeftJoin  => "ALL LEFT JOIN"
      case AllRightJoin => "ALL RIGHT JOIN"
      case AllInnerJoin => "ALL INNER JOIN"
    }

  private def tokenizeFiltering(maybeCondition: Option[TableColumn[Boolean]], keyword: String)(implicit database: Database): String =
    maybeCondition match {
      case None            => ""
      case Some(condition) => fast"$keyword ${tokenizeColumn(condition)}"
    }

  private def tokenizeGroupBy(groupBy: Option[GroupByQuery])(implicit database: Database): String = {
    val groupByColumns = groupBy match {
      case Some(GroupByQuery(usingColumns, _, _)) if usingColumns.nonEmpty =>
        Some(fast"GROUP BY ${tokenizeColumnsAliased(usingColumns)}")
      case _ =>
        None
    }
    val groupByMode = groupBy match {
      case Some(GroupByQuery(_, Some(mode), _)) => Some(mode match {
        case GroupByQuery.WithRollup => "WITH ROLLUP"
        case GroupByQuery.WithCube => "WITH CUBE"
      })
      case _ => None
    }
    val groupByWithTotals = groupBy match {
      case Some(GroupByQuery(_, _, true)) => Some("WITH TOTALS")
      case _ => None
    }
    (groupByColumns ++ groupByMode ++ groupByWithTotals).mkString(" ")
  }

  private def tokenizeOrderBy(orderBy: Seq[(AnyTableColumn, OrderingDirection)])(implicit database: Database): String =
    orderBy.toList match {
      case Nil | null => ""
      case _          => fast"ORDER BY ${tokenizeTuplesAliased(orderBy)}"
    }

  private def tokenizeLimit(limit: Option[Limit]): String =
    limit match {
      case None                      => ""
      case Some(Limit(size, offset)) => fast"LIMIT $offset, $size"
    }

  private def tokenizeColumnsAliased(columns: Seq[AnyTableColumn])(implicit database: Database): String =
    columns.map(aliasOrName).mkString(", ")

  private def tokenizeTuplesAliased(columns: Seq[(AnyTableColumn, OrderingDirection)])(implicit database: Database): String =
    columns.map {
        case (column, dir) =>
          aliasOrName(column) + " " + direction(dir)
      }
      .mkString(", ")

  private def aliasOrName(column: AnyTableColumn)(implicit database: Database) =
    column match {
      case EmptyColumn                   => ""
      case AliasedColumn(_, alias)       => alias
      case regularColumn: AnyTableColumn => regularColumn.name
    }

  private def direction(dir: OrderingDirection): String =
    dir match {
      case ASC  => "ASC"
      case DESC => "DESC"
    }

}
