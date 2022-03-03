package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.ClickhouseServerVersion
import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.misc.StringUtils
import com.crobox.clickhouse.time.{MultiDuration, TimeUnit, TotalDuration}
import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
//import scala.jdk.CollectionConverters._

case class TokenizeContext(version: ClickhouseServerVersion,
                           var joinNr: Int = 0,
                           var tableAliases: Map[Table, String] = Map.empty,
                           var useTableAlias: Boolean = false) {

  def incrementJoinNumber(): Unit = joinNr += 1

  def tableAlias(table: Table): String =
    if (useTableAlias) {
      tableAliases.getOrElse(table, {
        val alias = " AS " + ClickhouseStatement.quoteIdentifier("T" + (tableAliases.size + 1))
        tableAliases += (table -> alias)
        alias
      })
    } else ""

  def setTableAlias(value: Boolean): TokenizeContext =
    // change this object since we want to maintain all tableAliases over all joins
    if (true) {
      useTableAlias = value
      this
    } else {
      copy(useTableAlias = value)
    }

  def leftAlias(alias: Option[String]): String  = ClickhouseStatement.quoteIdentifier(alias.getOrElse("L" + joinNr))
  def rightAlias(alias: Option[String]): String = ClickhouseStatement.quoteIdentifier(alias.getOrElse("R" + joinNr))
}

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
    with URLFunctionTokenizer
    with EmptyFunctionTokenizer {

  private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  protected def tokenizeSeqCol(col1: Column, columns: Column*)(implicit ctx: TokenizeContext): String = {
    val prefix = if (columns.isEmpty) "" else ", "
    tokenizeColumn(col1) + prefix + tokenizeSeqCol(columns: _*)
  }

  protected def tokenizeSeqCol(columns: Column*)(implicit ctx: TokenizeContext): String =
    columns.map(tokenizeColumn).mkString(", ")

  override def toSql(query: InternalQuery,
                     formatting: Option[String] = Some("JSON"))(implicit ctx: TokenizeContext): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = StringUtils.removeRedundantWhitespaces(toRawSql(query) + formatSql)
    logger.debug(s"Generated sql [$sql]")
    sql
  }

  private[language] def toRawSql(query: InternalQuery)(implicit ctx: TokenizeContext): String =
    query match {
      case InternalQuery(select, from, prewhere, where, groupBy, having, join, orderBy, limit, union) =>
        s"""
           |${tokenizeSelect(select)}
           | ${tokenizeFrom(from)}
           | ${tokenizeJoin(select, from, join)}
           | ${tokenizeFiltering(prewhere, "PREWHERE")}
           | ${tokenizeFiltering(where, "WHERE")}
           | ${tokenizeGroupBy(groupBy)}
           | ${tokenizeFiltering(having, "HAVING")}
           | ${tokenizeOrderBy(orderBy)}
           | ${tokenizeLimit(limit)}
           | ${tokenizeUnionAll(union)}""".stripMargin
    }

  private def tokenizeUnionAll(unions: Seq[OperationalQuery])(implicit ctx: TokenizeContext): String =
    if (unions.nonEmpty) unions.map(q => s"UNION ALL ${toRawSql(q.internalQuery)}").mkString else ""

  private def tokenizeSelect(select: Option[SelectQuery])(implicit ctx: TokenizeContext): String =
    select match {
      case Some(s) => s"SELECT ${s.modifier} ${tokenizeColumns(s.columns)}"
      case _       => ""
    }

  private def tokenizeFrom(from: Option[FromQuery],
                           withPrefix: Boolean = true)(implicit ctx: TokenizeContext): String = {
    require(from != null)
    val fromClause = from match {
      case Some(query: InnerFromQuery)    => s"(${toRawSql(query.innerQuery.internalQuery).trim})"
      case Some(table: TableFromQuery[_]) => table.table.quoted + ctx.tableAlias(table.table)
      case _                              => return ""
    }

    val prefix = if (withPrefix) "FROM" else ""
    val alias  = from.flatMap(_.alias.map(s => " AS " + ClickhouseStatement.quoteIdentifier(s))).getOrElse("")
    val asF    = if (from.exists(_.finalized)) " FINAL" else ""
    s"$prefix $fromClause $alias $asF".trim
  }

  protected def tokenizeColumn(column: Column)(implicit ctx: TokenizeContext): String = {
    require(column != null)
    column match {
      case EmptyColumn => ""
      case alias: AliasedColumn[_] =>
        val originalColumnToken = tokenizeColumn(alias.original)
        if (originalColumnToken.isEmpty) alias.quoted else s"$originalColumnToken AS ${alias.quoted}"
      case tuple: TupleColumn[_]    => s"(${tuple.elements.map(tokenizeColumn).mkString(",")})"
      case col: ExpressionColumn[_] => tokenizeExpressionColumn(col)
      case col: Column              => col.quoted
    }
  }

  private def tokenizeExpressionColumn(inCol: ExpressionColumn[_])(implicit ctx: TokenizeContext): String =
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
      case col: EmptyFunction[_]             => tokenizeEmptyCol(col)
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
      case Conditional(cases, default, multiIf) =>
        if (multiIf) {
          s"${if (cases.size > 1) "multiIf" else "if"}(${cases
            .map(`case` => s"${tokenizeColumn(`case`.condition)}, ${tokenizeColumn(`case`.result)}")
            .mkString(", ")}, ${tokenizeColumn(default)})"
        } else {
          s"CASE ${cases
            .map(`case` => s"WHEN ${tokenizeColumn(`case`.condition)} THEN ${tokenizeColumn(`case`.result)}")
            .mkString(" ")} ELSE ${tokenizeColumn(default)} END"
        }
      case c: Const[_] => c.parsed
      case a @ _ =>
        throw new NotImplementedError(
          a.getClass.getCanonicalName + " with superclass " + a.getClass.getSuperclass.getCanonicalName + " could not be matched."
        )
    }

  private[language] def tokenizeTimeSeries(timeSeries: TimeSeries)(implicit ctx: TokenizeContext): String = {
    val column = tokenizeColumn(timeSeries.tableColumn)
    tokenizeDuration(timeSeries, column)
  }

  private def tokenizeDuration(timeSeries: TimeSeries, column: String): String = {
    val interval = timeSeries.interval
    val dateZone = determineZoneId(interval.rawStart)

    def convert(fn: String): String = s"$fn(toDateTime($column / 1000), '$dateZone')"

    def toDateTime(inner: String): String = s"toDateTime($inner, '$dateZone')"

    def toUnixTimestamp(inner: String): String = s"toUnixTimestamp($inner, '$dateZone')"

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

  // TODO this is a fallback to find a similar timezone when the provided interval does not have a set timezone id.
  // We should be able to disable this from the config and fail fast if we cannot determine the timezone for timeseries
  // (probably default to failing)
  private def determineZoneId(start: DateTime): String = {
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
  private def tokenizeJoin(select: Option[SelectQuery], from: Option[FromQuery], join: Option[JoinQuery])(
      implicit ctx: TokenizeContext
  ): String =
    join match {
      case Some(query) =>
        ctx.incrementJoinNumber()

        // we always need to provide an alias to the RIGHT side
        val right = query.other match {
          case table: TableFromQuery[_] => s"(SELECT * ${tokenizeFrom(Some(table))})"
          case query: InnerFromQuery    => tokenizeFrom(Some(query), withPrefix = false)
        }

        val leftAlias  = if (from.flatMap(_.alias).isEmpty) s"AS ${ctx.leftAlias(from.flatMap(_.alias))}" else ""
        val rightAlias = s"AS ${ctx.rightAlias(query.other.alias)}"

        s""" $leftAlias
           | ${if (query.global) "GLOBAL " else ""}
           | ${tokenizeJoinType(query.joinType)}
           | $right $rightAlias
           | ${tokenizeJoinKeys(select, from.get, query)}""".trim.stripMargin
          .replaceAll("\n", "")
          .replaceAll("\r", "")
      case None => ""
    }

  private def tokenizeJoinKeys(select: Option[SelectQuery], from: FromQuery, query: JoinQuery)(
      implicit ctx: TokenizeContext
  ): String = {

    val using = query.using.filterNot {
      case EmptyColumn => true
      case _           => false
    }
    query.joinType match {
      case CrossJoin =>
        assert(using.isEmpty, "When using CrossJoin, no using columns should be provided")
        assert(query.on.isEmpty, "When using CrossJoin, no on conditions should be provided")
        ""
      case _ =>
        assert(using.nonEmpty || query.on.nonEmpty, s"No USING or ON provided for joinType: ${query.joinType}")
        assert(!(using.nonEmpty && query.on.nonEmpty), s"Both USING and ON provided for joinType: ${query.joinType}")

        if (using.nonEmpty) {
          // TOKENIZE USING
          if (using.size == 1) s"USING ${using.head.name}"
          else s"USING (${using.map(_.name).mkString(",")})"
        } else if (query.on.nonEmpty) {
          // TOKENIZE ON. If the fromClause is a TABLE, we need to check on aliases!
          "ON " + query.on
            .map(cond => {
              val left = verifyOnCondition(select, from, cond.left)
              s"${ctx.leftAlias(from.alias)}.$left ${cond.operator} ${ctx.rightAlias(query.other.alias)}.${cond.right.name}"
            })
            .mkString(" AND ")
        } else ""
    }
  }

  private def verifyOnCondition(select: Option[SelectQuery], from: FromQuery, joinKey: Column): String =
    from match {
      case _: TableFromQuery[_] =>
        // check if joinKey is an existing DB field or an alias!
        select
          .map(_.columns)
          .getOrElse(Seq.empty)
          .flatMap {
            case x: AliasedColumn[_] => if (x.alias == joinKey.name) Option(x.original.name) else None
            case _                   => None
          }
          .headOption
          .getOrElse(joinKey.name)
      case _ => joinKey.name
    }

  private[language] def tokenizeColumns(columns: Seq[Column])(implicit ctx: TokenizeContext): String =
    columns
      .filterNot {
        case EmptyColumn => true
        case _           => false
      }
      .map(tokenizeColumn)
      .mkString(", ")

  /**
   * https://clickhouse.tech/docs/en/sql-reference/statements/select/join/
   */
  private def tokenizeJoinType(joinType: JoinQuery.JoinType): String =
    joinType match {
      // Standard SQL JOIN https://en.wikipedia.org/wiki/Join_(SQL)
      case InnerJoin      => "INNER JOIN"
      case LeftOuterJoin  => "LEFT OUTER JOIN"
      case RightOuterJoin => "RIGHT OUTER JOIN"
      case FullOuterJoin  => "FULL OUTER JOIN"
      case CrossJoin      => "CROSS JOIN"

      // custom clickhouse
      case AllInnerJoin  => "ALL INNER JOIN"
      case AllLeftJoin   => "ALL LEFT JOIN"
      case AllRightJoin  => "ALL RIGHT JOIN"
      case AntiLeftJoin  => "ANTI LEFT JOIN"
      case AntiRightJoin => "ANTI RIGHT JOIN"
      case AnyInnerJoin  => "ANY INNER JOIN"
      case AnyLeftJoin   => "ANY LEFT JOIN"
      case AnyRightJoin  => "ANY RIGHT JOIN"
      case AsOfJoin      => "ASOF JOIN"
      case AsOfLeftJoin  => "ASOF LEFT JOIN"
      case SemiLeftJoin  => "SEMI LEFT JOIN"
      case SemiRightJoin => "SEMI RIGHT JOIN"
    }

  private def tokenizeFiltering(maybeCondition: Option[TableColumn[Boolean]],
                                keyword: String)(implicit ctx: TokenizeContext): String =
    maybeCondition match {
      case None            => ""
      case Some(condition) =>
        //s"$keyword ${tokenizeColumn(condition)}"
        s"$keyword ${StringUtils.removeSurroundingBrackets(tokenizeColumn(condition).trim)}"
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

  private def tokenizeOrderBy(orderBy: Seq[(Column, OrderingDirection)])(implicit ctx: TokenizeContext): String =
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

  private def tokenizeTuplesAliased(columns: Seq[(Column, OrderingDirection)])(implicit ctx: TokenizeContext): String =
    columns
      .map {
        case (column, dir) => tokenizeColumn(column) + " " + direction(dir)
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
