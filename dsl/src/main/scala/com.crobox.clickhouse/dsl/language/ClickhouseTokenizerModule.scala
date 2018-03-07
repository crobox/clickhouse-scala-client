package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl
import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.time.TimeUnit.{Quarter, Total, Year}
import com.crobox.clickhouse.time.{MultiDuration, SimpleDuration, TimeUnit}
import com.dongxiguo.fastring.Fastring.Implicits._
import com.typesafe.scalalogging.Logger
import org.joda.time.DateTimeZone
import org.slf4j.LoggerFactory

trait ClickhouseTokenizerModule extends TokenizerModule {
  private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def toSql(query: InternalQuery,
                     formatting: Option[String] = Some("JSON"))(implicit database: Database): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = (toRawSql(query) + formatSql).replaceAll("\n", "").replaceAll("\r", "").trim().replaceAll(" +", " ")
    logger.debug(fast"Generated sql [$sql]")
    sql
  }

  private def toRawSql(query: InternalQuery)(implicit database: Database): String =
    //    require(query != null) because parallel query is null
    query match {
      case InternalQuery(select, from, where, groupBy, having, join, orderBy, limit) =>
        fast"""
           |${tokenizeSelect(select)}
           | FROM ${tokenizeFrom(from)}
           | ${tokenizeJoin(join)}
           | ${tokenizeFiltering(where, "WHERE")}
           | ${tokenizeGroupBy(groupBy)}
           | ${tokenizeFiltering(having, "HAVING")}
           | ${tokenizeOrderBy(orderBy)}
           | ${tokenizeLimit(limit)}"""
          .toString
          .stripMargin
    }

  private def tokenizeSelect(select: Option[SelectQuery]) = {
    select match {
      case Some(s) => fast"SELECT ${s.modifier} ${tokenizeColumns(s.columns)}"
      case _ => ""
    }
  }

  private def tokenizeFrom(from: Option[FromQuery])(implicit database: Database) = {
    require(from != null)
    from match {
      case Some(fromClause: InnerFromQuery)                => fast"(${toRawSql(fromClause.innerQuery.internalQuery)})"
      case Some(TableFromQuery(table: Table, None))        => fast"$database.${table.name}"
      case Some(TableFromQuery(table: Table, Some(altDb))) => fast"$altDb.${table.name}"
      case _ => ""
    }
  }

  protected def tokenizeColumn(column: AnyTableColumn): String = {
    require(column != null)
    column match {
      case AliasedColumn(original, alias) => fast"${tokenizeColumn(original)} AS $alias"
      case tuple: TupleColumn[_]          => fast"(${tuple.elements.map(tokenizeColumn).mkString(",")})"
      case Count(countColumn)             => fast"count(${countColumn.map(tokenizeColumn).getOrElse("")})"
      case c: Const[_]                    => c.parsed
      case CountIf(expressionColumn: ExpressionColumn[_]) =>
        fast"countIf(${tokenizeColumn(expressionColumn)})"
      case CountIf(comparison: Comparison) =>
        fast"countIf(${tokenizeCondition(comparison)})"
      case ArrayJoin(tableColumn)         => fast"arrayJoin(${tokenizeColumn(tableColumn)})"
      case GroupUniqArray(tableColumn)    => fast"groupUniqArray(${tokenizeColumn(tableColumn)})"
      case All()                          => "*"
      case UniqIf(tableColumn, expressionColumn: ExpressionColumn[_]) =>
        fast"uniqIf(${tokenizeColumn(tableColumn)}, ${tokenizeColumn(expressionColumn)})"
      case UniqIf(tableColumn, expressionColumn: Comparison) =>
        fast"uniqIf(${tokenizeColumn(tableColumn)}, ${tokenizeCondition(expressionColumn)})"
      case BooleanInt(tableColumn, value) => fast"${tokenizeColumn(tableColumn)} = $value"
      case Empty(tableColumn)             => fast"empty(${tokenizeColumn(tableColumn)})"
      case NotEmpty(tableColumn)          => fast"notEmpty(${tokenizeColumn(tableColumn)})"
      case UInt64(tableColumn)            => fast"toUInt64(${tokenizeColumn(tableColumn)})"
      case Uniq(tableColumn)              => fast"uniq(${tokenizeColumn(tableColumn)})"
      case UniqState(tableColumn)         => fast"uniqState(${tokenizeColumn(tableColumn)})"
      case UniqMerge(tableColumn)         => fast"uniqMerge(${tokenizeColumn(tableColumn)})"
      case Sum(tableColumn)               => fast"sum(${tokenizeColumn(tableColumn)})"
      case Min(tableColumn)               => fast"min(${tokenizeColumn(tableColumn)})"
      case Max(tableColumn)               => fast"max(${tokenizeColumn(tableColumn)})"
      case LowerCaseColumn(tableColumn)   => fast"lowerUTF8(${tokenizeColumn(tableColumn)})"
      case timeSeries: TimeSeries         => tokenizeTimeSeries(timeSeries)
      case Conditional(cases, default) =>
        fast"CASE ${cases
          .map(ccase => fast"WHEN ${tokenizeCondition(ccase.condition)} THEN ${tokenizeColumn(ccase.column)}")
          .mkString(" ")} ELSE ${tokenizeColumn(default)} END"
      case regularColumn: AnyTableColumn => regularColumn.name
    }
  }

  private def tokenizeTimeSeries(timeSeries: TimeSeries): String = {
    val column = tokenizeColumn(timeSeries.tableColumn)
    val zone   = timeSeries.interval.getStart.getZone
    tokenizeDuration(timeSeries, column, zone)
  }

  private def tokenizeDuration(timeSeries: TimeSeries, column: String, dateZone: DateTimeZone) = {
    val interval = timeSeries.interval
    val zoneId   = dateZone.getID
    interval.duration match {
      case MultiDuration(value, TimeUnit.Month) =>
        fast"concat(toString(intDiv(toRelativeMonthNum(toDateTime($column / 1000),'$zoneId'), $value) * $value),'_$zoneId')"
      case MultiDuration(_, Quarter) =>
        fast"concat(toString(toStartOfQuarter(toDateTime($column / 1000),'$zoneId')),'_$zoneId')"
      case MultiDuration(_, Year) =>
        fast"concat(toString(toStartOfYear(toDateTime($column / 1000),'$zoneId')),'_$zoneId')"
      case SimpleDuration(Total) => fast"${interval.getStartMillis}"
      //        handles seconds/minutes/hours/days/weeks
      case multiDuration: MultiDuration =>
        //        for fixed duration we calculate the milliseconds for the start of a sub interval relative to our predefined interval start. The first subinterval start would be `interval.startOfInterval()`
        //        if using weeks this would give the milliseconds of the start of the first day of the week, for days it would be the start of the day and so on.
        val intervalStartMillis = interval.startOfInterval().getMillis
        fast"((intDiv($column - $intervalStartMillis, ${multiDuration.millis()}) * ${multiDuration.millis()}) + $intervalStartMillis)"
    }
  }

  //  Table joins are tokenized as select * because of https://github.com/yandex/ClickHouse/issues/635
  private def tokenizeJoin(option: Option[JoinQuery])(implicit database: Database): String =
    option match {
      case None =>
        ""
      case Some(JoinQuery(joinType,tableJoin: TableFromQuery[_],usingCols)) =>
        fast"${tokenizeJoinType(joinType)} (SELECT * FROM ${tokenizeFrom(Some(tableJoin))}) USING ${tokenizeColumns(usingCols)}"
      case Some(JoinQuery(joinType,innerJoin: InnerFromQuery,usingCols)) =>
        fast"${tokenizeJoinType(joinType)} ${tokenizeFrom(Some(innerJoin))} USING ${tokenizeColumns(usingCols)}"
    }

  private def tokenizeColumns(columns: Set[AnyTableColumn]): String =
    columns.map(tokenizeColumn).mkString(", ")

  private def tokenizeColumns(columns: Seq[AnyTableColumn]): String =
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

  private def tokenizeFiltering(maybeCondition: Option[dsl.Comparison], keyword: String): String =
    maybeCondition match {
      case None            => ""
      case Some(condition) => fast"$keyword ${tokenizeCondition(condition)}"
    }

  protected def tokenizeCondition(condition: Comparison): String =
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

  private def tokenizeGroupBy(groupBy: Seq[AnyTableColumn]): String =
    groupBy.toList match {
      case Nil | null => ""
      case _          => fast"GROUP BY ${tokenizeColumnsAliased(groupBy)}"
    }

  private def tokenizeOrderBy(orderBy: Seq[(AnyTableColumn, OrderingDirection)]): String =
    orderBy.toList match {
      case Nil | null => ""
      case _          => fast"ORDER BY ${tokenizeTuplesAliased(orderBy)}"
    }

  private def tokenizeLimit(limit: Option[Limit]): String =
    limit match {
      case None                      => ""
      case Some(Limit(size, offset)) => fast" LIMIT $offset, $size"
    }

  private def tokenizeColumnsAliased(columns: Seq[AnyTableColumn]): String =
    columns.map(aliasOrName).mkString(", ")

  private def tokenizeTuplesAliased(columns: Seq[(AnyTableColumn, OrderingDirection)]): String =
    columns
      .map {
        case (column, dir) =>
          aliasOrName(column) + " " + direction(dir)
      }
      .mkString(", ")

  private def aliasOrName(column: AnyTableColumn) =
    column match {
      case AliasedColumn(_, alias)       => alias
      case regularColumn: AnyTableColumn => regularColumn.name
    }

  private def direction(dir: OrderingDirection): String =
    dir match {
      case ASC  => "ASC"
      case DESC => "DESC"
    }

}
