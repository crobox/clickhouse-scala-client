package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl
import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.column.ClickhouseColumnFunctions
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.time.{MultiDuration, SimpleDuration, TimeUnit}
import com.dongxiguo.fastring.Fastring.Implicits._
import com.google.common.base.Strings
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
    with ClickhouseColumnFunctions {

  private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  protected def tokenizeSeqCol[C <: TableColumn[_]](colSeq: Seq[C]): String = {
    val prefix = if (colSeq.isEmpty) "" else ", "
    colSeq.map(tokenizeColumn).mkString(", ")
  }

  override def toSql(query: InternalQuery,
                     formatting: Option[String] = Some("JSON"))(implicit database: Database): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = (toRawSql(query) + formatSql).trim().replaceAll(" +", " ")
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
           |${tokenizeUnionAll(union)}""".toString.trim.stripMargin.replaceAll("\n", "").replaceAll("\r", "")
    }

  private def tokenizeUnionAll(unions : Seq[OperationalQuery])(implicit database: Database) = {
    if (unions.nonEmpty) {
      unions.map(q =>
        fast"""UNION ALL
               | ${toRawSql(q.internalQuery)}""".toString.stripMargin).mkString
    } else {
      ""
    }
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
      case agg: AggregateFunction[_]     => tokenizeAggregateFunction(agg)
      case col: TypeCastColumn[_]        => tokenizeTypeCastColumn(col)
      case col: DateTimeFunctionCol[_]   => tokenizeDateTimeColumn(col)
      case col: DateTimeConst[_]         => tokenizeDateTimeConst(col)
      case col: ArithmeticFunctionCol[_] => tokenizeArithmeticFunctionColumn(col)
      case col: ArithmeticFunctionOp[_]  => tokenizeArithmeticFunctionOperator(col)
      case ArrayJoin(tableColumn)        => fast"arrayJoin(${tokenizeColumn(tableColumn.column)})"
      case All()                         => "*"
      case LowerCaseColumn(tableColumn)  => fast"lowerUTF8(${tokenizeColumn(tableColumn)})"
      case Conditional(cases, default) =>
        fast"CASE ${cases
          .map(ccase => fast"WHEN ${tokenizeCondition(ccase.condition)} THEN ${tokenizeColumn(ccase.column)}")
          .mkString(" ")} ELSE ${tokenizeColumn(default)} END"
      case c: Const[_] => c.parsed
      case QueryColumn(query) => s"(${toRawSql(query.internalQuery)})"
    }

  private[language] def tokenizeTimeSeries(timeSeries: TimeSeries)(implicit database: Database): String = {
    val column = tokenizeColumn(timeSeries.tableColumn)
    tokenizeDuration(timeSeries, column)
  }

  private def tokenizeDuration(timeSeries: TimeSeries, column: String) = {
    val interval = timeSeries.interval
    interval.duration match {
      case MultiDuration(value, TimeUnit.Month) =>
        val dateZone = determineZoneId(interval.rawStart)
        fast"concat(toString(intDiv(toRelativeMonthNum(toDateTime($column / 1000),'$dateZone'), $value) * $value),'_$dateZone')"
      case MultiDuration(_, TimeUnit.Quarter) =>
        val dateZone = determineZoneId(interval.rawStart)
        fast"concat(toString(toStartOfQuarter(toDateTime($column / 1000),'$dateZone')),'_$dateZone')"
      case MultiDuration(_, TimeUnit.Year) =>
        val dateZone = determineZoneId(interval.rawStart)
        fast"concat(toString(toStartOfYear(toDateTime($column / 1000),'$dateZone')),'_$dateZone')"
      case SimpleDuration(TimeUnit.Total) => fast"${interval.getStartMillis}"
      //        handles seconds/minutes/hours/days/weeks
      case multiDuration: MultiDuration =>
        //        for fixed duration we calculate the milliseconds for the start of a sub interval relative to our predefined interval start. The first subinterval start would be `interval.startOfInterval()`
        //        if using weeks this would give the milliseconds of the start of the first day of the week, for days it would be the start of the day and so on.
        val intervalStartMillis = interval.startOfInterval().getMillis
        fast"((intDiv($column - $intervalStartMillis, ${multiDuration.millis()}) * ${multiDuration.millis()}) + $intervalStartMillis)"
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

  private def tokenizeFiltering(maybeCondition: Option[TableColumn[Boolean]], keyword: String)(implicit database: Database): String =
    maybeCondition match {
      case None            => ""
      case Some(condition) => fast"$keyword ${tokenizeCondition(condition)}"
    }

  protected def tokenizeCondition(condition: TableColumn[Boolean])(implicit database: Database): String =
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
      case QueryColumn(query) => s"(${toRawSql(query.internalQuery).trim.replaceAll(" +", " ")})"
      case regularColumn: AnyTableColumn => regularColumn.name
    }

  private def direction(dir: OrderingDirection): String =
    dir match {
      case ASC  => "ASC"
      case DESC => "DESC"
    }

}
