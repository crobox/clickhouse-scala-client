package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl
import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.time.TimeUnit.{Quarter, Total, Year}
import com.crobox.clickhouse.time.{MultiDuration, SimpleDuration, TimeUnit}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.joda.time.DateTimeZone
import org.slf4j.LoggerFactory

import scala.collection.mutable

trait ClickhouseTokenizerModule extends TokenizerModule {

  private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def toSql(query: InternalQuery,
                     formatting: Option[String] = Some("JSON"))(implicit database: Database): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = (toRawSql(query) + formatSql).replaceAll("\n", "").replaceAll("\r", "").trim().replaceAll(" +", " ")
    logger.debug(s"Generated sql [$sql]")
    sql
  }

  private def toRawSql(query: InternalQuery)(implicit database: Database): String =
    //    require(query != null) because parallel query is null
    query match {
      case InternalQuery(selectQuery, from, where, groupBy, having, join, orderBy, limit) =>
        s"""
           |SELECT ${selectQuery.modifier} ${tokenizeColumns(
             selectQuery.columns
           )} FROM
           | ${tokenizeFrom(from)}
           | ${tokenizeJoin(join)}
           | ${tokenizeFiltering(where, "WHERE")}
           | ${tokenizeGroupBy(groupBy)}
           | ${tokenizeFiltering(having, "HAVING")}
           | ${tokenizeOrderBy(orderBy)}
           | ${tokenizeLimit(limit)}""".stripMargin
    }

  private def tokenizeFrom(from: FromQuery)(implicit database: Database) = {
    require(from != null)
    from match {
      case fromClause: InnerFromQuery                   => s"(${toRawSql(fromClause.innerQuery.internalQuery)})"
      case TableFromQuery(_, table: Table, None)        => s"$database.${table.name}"
      case TableFromQuery(_, table: Table, Some(altDb)) => s"$altDb.${table.name}"
    }
  }

  protected def tokenizeColumn(column: AnyTableColumn): String = {
    require(column != null)
    column match {
      case AliasedColumn(original, alias) => s"${tokenizeColumn(original)} AS $alias"
      case tuple: TupleColumn[_]          => s"(${tuple.elements.map(tokenizeColumn).mkString(",")})"
      case Count(countColumn)             => s"count(${countColumn.map(tokenizeColumn).getOrElse("")})"
      case c: Const[_]                    => c.parsed
      case CountIf(expressionColumn)      => s"countIf(${tokenizeColumn(expressionColumn)})"
      case ArrayJoin(tableColumn)         => s"arrayJoin(${tokenizeColumn(tableColumn)})"
      case GroupUniqArray(tableColumn)    => s"groupUniqArray(${tokenizeColumn(tableColumn)})"
      case All()                          => "*"
      case UniqIf(tableColumn, expressionColumn) =>
        s"uniqIf(${tokenizeColumn(tableColumn)}, ${tokenizeColumn(expressionColumn)})"
      case BooleanInt(tableColumn, value) => s"${tokenizeColumn(tableColumn)} = $value"
      case Empty(tableColumn)             => s"empty(${tokenizeColumn(tableColumn)})"
      case NotEmpty(tableColumn)          => s"notEmpty(${tokenizeColumn(tableColumn)})"
      case UInt64(tableColumn)            => s"toUInt64(${tokenizeColumn(tableColumn)})"
      case Uniq(tableColumn)              => s"uniq(${tokenizeColumn(tableColumn)})"
      case UniqState(tableColumn)         => s"uniqState(${tokenizeColumn(tableColumn)})"
      case UniqMerge(tableColumn)         => s"uniqMerge(${tokenizeColumn(tableColumn)})"
      case Sum(tableColumn)               => s"sum(${tokenizeColumn(tableColumn)})"
      case Min(tableColumn)               => s"min(${tokenizeColumn(tableColumn)})"
      case Max(tableColumn)               => s"max(${tokenizeColumn(tableColumn)})"
      case LowerCaseColumn(tableColumn)   => s"lowerUTF8(${tokenizeColumn(tableColumn)})"
      case timeSeries: TimeSeries         => tokenizeTimeSeries(timeSeries)
      case Conditional(cases, default) =>
        s"CASE ${cases
          .map(ccase => s"WHEN ${tokenizeCondition(ccase.condition)} THEN ${tokenizeColumn(ccase.column)}")
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
        s"concat(toString(intDiv(toRelativeMonthNum(toDateTime($column / 1000),'$zoneId'), $value) * $value),'_$zoneId')"
      case MultiDuration(_, Quarter) =>
        s"concat(toString(toStartOfQuarter(toDateTime($column / 1000),'$zoneId')),'_$zoneId')"
      case MultiDuration(_, Year) =>
        s"concat(toString(toStartOfYear(toDateTime($column / 1000),'$zoneId')),'_$zoneId')"
      case SimpleDuration(Total) => s"${interval.getStartMillis}"
      //        handles seconds/minutes/hours/days/weeks
      case multiDuration: MultiDuration =>
        //        for fixed duration we calculate the milliseconds for the start of a sub interval relative to our predefined interval start. The first subinterval start would be `interval.startOfInterval()`
        //        if using weeks this would give the milliseconds of the start of the first day of the week, for days it would be the start of the day and so on.
        val intervalStartMillis = interval.startOfInterval().getMillis
        s"((intDiv($column - $intervalStartMillis, ${multiDuration.millis()}) * ${multiDuration.millis()}) + $intervalStartMillis)"
    }
  }

  //  Table joins are tokenized as select * because of https://github.com/yandex/ClickHouse/issues/635
  private def tokenizeJoin(option: Option[JoinQuery])(implicit database: Database): String =
    option match {
      case None => ""
      case Some(join) =>
        join match {
          case tableJoin: TableJoinedQuery[_, _] =>
            s"${tokenizeJoinType(tableJoin.`type`)} (${toRawSql((select(all()) from tableJoin.table).internalQuery)}) USING ${tokenizeColumns(tableJoin.usingColumns)}"
          case innerQueryJoin: InnerJoinedQuery =>
            s"${tokenizeJoinType(innerQueryJoin.`type`)} (${toRawSql(innerQueryJoin.joinQuery.internalQuery)}) USING ${tokenizeColumns(innerQueryJoin.usingColumns)}"
        }
    }

  private def tokenizeColumns(columns: Set[AnyTableColumn]): String =
    columns.map(tokenizeColumn).mkString(", ")

  private def tokenizeColumns(columns: mutable.LinkedHashSet[AnyTableColumn]): String =
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
      case Some(condition) => s"$keyword ${tokenizeCondition(condition)}"
    }

  protected def tokenizeCondition(condition: Comparison): String =
    condition match {
      case _: NoOpComparison                             => ""
      case ColRefColumnComparison(left, operator, right) => s"${aliasOrName(left)} $operator ${aliasOrName(right)}"
      case vcc @ ValueColumnComparison(left, operator, right) =>
        s"${aliasOrName(left)} $operator ${vcc.queryValueEvidence(right)}"
      case FunctionColumnComparison(function, column) => s"$function(${aliasOrName(column)})"
      case HigherOrderFunction(function, comparisonColumn, comparison, column) =>
        s"$function(${tokenizeColumn(comparisonColumn)} -> ${tokenizeCondition(comparison)}, ${tokenizeColumn(column)})"
      case ChainableColumnCondition(_: NoOpComparison, _, _: NoOpComparison) => ""
      case ChainableColumnCondition(left, _, _: NoOpComparison)              => s"${tokenizeCondition(left)}"
      case ChainableColumnCondition(_: NoOpComparison, _, right)             => s"${tokenizeCondition(right)}"
      case ChainableColumnCondition(left, operator, right) if operator == "OR" =>
        s"(${tokenizeCondition(left)} $operator ${tokenizeCondition(right)})"
      case ChainableColumnCondition(left, operator, right) =>
        s"${tokenizeCondition(left)} $operator ${tokenizeCondition(right)}"
    }

  private def tokenizeGroupBy(groupBy: mutable.Set[AnyTableColumn]): String =
    groupBy.toList match {
      case Nil | null => ""
      case _          => s"GROUP BY ${tokenizeColumnsAliased(groupBy)}"
    }

  private def tokenizeOrderBy(orderBy: mutable.Set[(AnyTableColumn, OrderingDirection)]): String =
    orderBy.toList match {
      case Nil | null => ""
      case _          => s"ORDER BY ${tokenizeTuplesAliased(orderBy)}"
    }

  private def tokenizeLimit(limit: Option[Limit]): String =
    limit match {
      case None                      => ""
      case Some(Limit(size, offset)) => s" LIMIT $offset, $size"
    }

  private def tokenizeColumnsAliased(columns: mutable.Set[AnyTableColumn]): String =
    columns.map(aliasOrName).mkString(", ")

  private def tokenizeTuplesAliased(columns: mutable.Set[(AnyTableColumn, OrderingDirection)]): String =
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
