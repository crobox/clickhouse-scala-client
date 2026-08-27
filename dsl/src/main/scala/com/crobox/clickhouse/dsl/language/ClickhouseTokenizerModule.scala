package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.time.{MultiDuration, TimeUnit, TotalDuration}
import com.typesafe.scalalogging.Logger
import java.time.{ZoneId, ZonedDateTime}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/** Tokens that are fixed by ClickHouse's grammar rather than by anything the caller chooses. */
private[language] object Tokens {

  /** Separator between a function's arguments. */
  val Delimiter: String = ", "
}

/**
 * State accumulated while rendering one query: join numbering and the table aliases handed out so far.
 *
 * Deliberately not a case class. The fields change as tokenization walks the query, so a derived `equals`/`hashCode`
 * would drift under its own callers and `copy` would hand out a snapshot that silently diverges from the original. Use
 * one instance per rendered query -- sharing one leaks join numbers and aliases between unrelated queries.
 */
class TokenizeContext {

  private var joinNr: Int                      = 0
  private var tableAliases: Map[Table, String] = Map.empty
  private var useTableAlias: Boolean           = false

  /**
   * Claims this join's number. The caller must hold on to the result rather than reading it back later: tokenizing a
   * join's right-hand side advances the counter again for every join nested inside it.
   */
  def nextJoinNumber(): Int = {
    joinNr += 1
    joinNr
  }

  def tableAlias(table: Table): String =
    if (useTableAlias) {
      tableAliases.getOrElse(
        table, {
          val alias = " AS " + ClickhouseStatement.quoteIdentifier("T" + (tableAliases.size + 1))
          tableAliases += (table -> alias)
          alias
        }
      )
    } else ""

  /**
   * Runs `f` with table aliasing turned on or off, restoring the previous setting afterwards.
   *
   * Scoped rather than sticky: as a latching flag this made a subexpression's SQL depend on what happened to be
   * tokenized before it, so the same `GLOBAL IN` rendered with or without an alias depending on its siblings.
   * `tableAliases` is deliberately not rolled back -- an alias already emitted has to keep meaning the same table for
   * the rest of the query.
   */
  def withTableAlias[A](value: Boolean)(f: => A): A = {
    val previous = useTableAlias
    useTableAlias = value
    try f
    finally useTableAlias = previous
  }

  def leftAlias(alias: Option[String], joinNr: Int): String =
    ClickhouseStatement.quoteIdentifier(alias.getOrElse("L" + joinNr))

  def rightAlias(alias: Option[String], joinNr: Int): String =
    ClickhouseStatement.quoteIdentifier(alias.getOrElse("R" + joinNr))
}

object TokenizeContext {
  def apply(): TokenizeContext = new TokenizeContext
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
    with DistanceFunctionTokenizer
    with EncodingFunctionTokenizer
    with HashFunctionTokenizer
    with HigherOrderFunctionTokenizer
    with IPFunctionTokenizer
    with InFunctionTokenizer
    with JsonFunctionTokenizer
    with LogicalFunctionTokenizer
    with MathematicalFunctionTokenizer
    with MiscellaneousFunctionTokenizer
    with NullableFunctionTokenizer
    with RandomFunctionTokenizer
    with RoundingFunctionTokenizer
    with SplitMergeFunctionTokenizer
    with StringFunctionTokenizer
    with StringSearchFunctionTokenizer
    with TypeCastFunctionTokenizer
    with URLFunctionTokenizer
    with WindowFunctionTokenizer
    with EmptyFunctionTokenizer {

  private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  protected def tokenizeSeqCol(col1: Column, columns: Column*)(implicit ctx: TokenizeContext): String = {
    val prefix = if (columns.isEmpty) "" else ", "
    tokenizeColumn(col1) + prefix + tokenizeSeqCol(columns: _*)
  }

  protected def tokenizeSeqCol(columns: Column*)(implicit ctx: TokenizeContext): String =
    columns.map(tokenizeColumn).mkString(Tokens.Delimiter)

  override def toSql(query: InternalQuery, formatting: Option[String] = Some("JSON"))(implicit
      ctx: TokenizeContext
  ): String = {
    val formatSql = formatting.map(fmt => " FORMAT " + fmt).getOrElse("")
    val sql       = toRawSql(query) + formatSql
    logger.debug(s"Generated sql [$sql]")
    sql
  }

  override def toExplainSql(
      kind: ExplainKind,
      query: InternalQuery,
      options: Seq[(String, String)] = Seq.empty
  )(implicit ctx: TokenizeContext): String = {
    // Same `key = value` form as query-level SETTINGS, but positioned before the query rather than after it.
    val optionsSql =
      if (options.isEmpty) "" else options.map { case (k, v) => s"$k = $v" }.mkString(" ", Tokens.Delimiter, "")
    val sql = s"EXPLAIN ${kind.keyword}$optionsSql ${toRawSql(query)}"
    logger.debug(s"Generated sql [$sql]")
    sql
  }

  override def toStatementSql(statement: Statement)(implicit ctx: TokenizeContext): String = {
    // Between the table and the action for ALTER, but after the WHERE's table for DELETE FROM -- the two statements
    // put the clause in different places.
    def onCluster(name: Option[String]): String =
      name.map(c => s" ON CLUSTER ${ClickhouseStatement.quoteIdentifier(c)}").getOrElse("")

    val sql = statement match {
      case DeleteFrom(table, where, cluster) =>
        s"DELETE FROM ${table.quoted}${onCluster(cluster)} WHERE ${tokenizeColumn(where)}"
      case AlterDelete(table, where, cluster) =>
        s"ALTER TABLE ${table.quoted}${onCluster(cluster)} DELETE WHERE ${tokenizeColumn(where)}"
      case AlterUpdate(table, assignments, where, cluster) =>
        val sets = assignments
          .map { case Assignment(column, value) => s"${aliasOrName(column)} = ${tokenizeColumn(value)}" }
          .mkString(Tokens.Delimiter)
        s"ALTER TABLE ${table.quoted}${onCluster(cluster)} UPDATE $sets WHERE ${tokenizeColumn(where)}"
      case DropPartition(table, partition, cluster) =>
        val target = partition match {
          case PartitionRef.Id(id)           => s"ID ${"'" + ClickhouseStatement.escape(id) + "'"}"
          case PartitionRef.Expression(expr) => tokenizeColumn(expr)
        }
        s"ALTER TABLE ${table.quoted}${onCluster(cluster)} DROP PARTITION $target"
    }
    logger.debug(s"Generated sql [$sql]")
    sql
  }

  private def removeSurroundingBrackets(value: String): String =
    if (value.startsWith("(") && value.endsWith(")") && value.count(_ == '(') == 1 && value.count(_ == ')') == 1) {
      value.substring(1, value.length - 1)
    } else value

  private[language] def toRawSql(query: InternalQuery)(implicit ctx: TokenizeContext): String =
    query match {
      // Clause order follows https://clickhouse.com/docs/sql-reference/statements/select -- SAMPLE is rendered by
      // tokenizeFrom, since it belongs to the table expression alongside FINAL.
      case InternalQuery(
            select,
            from,
            prewhere,
            where,
            groupBy,
            having,
            joins,
            arrayJoin,
            orderBy,
            limit,
            limitBy,
            combinations,
            settings,
            withEntries,
            interpolate,
            windows,
            qualify
          ) =>
        // The left table's alias is emitted by tokenizeJoin, and ARRAY JOIN has to land between that alias and the
        // JOIN keyword, so it is threaded through rather than placed here. Without a join there is no alias to follow.
        val arrayJoinSql = tokenizeArrayJoin(arrayJoin)
        Seq(
          tokenizeWith(withEntries),
          tokenizeSelect(select),
          tokenizeFrom(from),
          if (joins.nonEmpty) "" else arrayJoinSql,
          tokenizeJoins(select, from, joins, arrayJoinSql),
          tokenizeFiltering(prewhere, "PREWHERE"),
          tokenizeFiltering(where, "WHERE"),
          tokenizeGroupBy(groupBy),
          tokenizeFiltering(having, "HAVING"),
          tokenizeWindows(windows),
          tokenizeFiltering(qualify, "QUALIFY"),
          tokenizeOrderBy(orderBy),
          tokenizeInterpolate(interpolate),
          tokenizeLimitBy(limitBy),
          tokenizeLimit(limit),
          tokenizeSettings(settings),
          tokenizeSetOperations(combinations)
        ).filter(_.nonEmpty).mkString(" ")
    }

  private def tokenizeWith(entries: Seq[WithEntry])(implicit ctx: TokenizeContext): String =
    if (entries.isEmpty) ""
    else
      entries
        .map {
          // The two expression forms read `<value> AS <name>`; the CTE form reads the other way round.
          case WithExpression(expression, name) =>
            s"${tokenizeColumn(expression)} AS ${ClickhouseStatement.quoteIdentifier(name)}"
          case WithScalarQuery(query, name) =>
            s"(${toRawSql(query.internalQuery)}) AS ${ClickhouseStatement.quoteIdentifier(name)}"
          case WithTable(name, query) =>
            s"${ClickhouseStatement.quoteIdentifier(name)} AS (${toRawSql(query.internalQuery)})"
        }
        .mkString("WITH ", Tokens.Delimiter, "")

  private def tokenizeWindows(windows: Seq[NamedWindow])(implicit ctx: TokenizeContext): String =
    if (windows.isEmpty) ""
    else
      windows
        .map(w => s"${ClickhouseStatement.quoteIdentifier(w.name)} AS (${tokenizeWindowSpec(w.spec)})")
        .mkString("WINDOW ", Tokens.Delimiter, "")

  private def tokenizeWindowSpec(spec: WindowSpec)(implicit ctx: TokenizeContext): String =
    Seq(
      if (spec.partitionBy.isEmpty) "" else s"PARTITION BY ${tokenizeColumns(spec.partitionBy)}",
      tokenizeOrderBy(spec.orderBy),
      spec.frame.map(tokenizeWindowFrame).getOrElse("")
    ).filter(_.nonEmpty).mkString(" ")

  private def tokenizeWindowFrame(frame: WindowFrame): String = {
    def bound(b: FrameBound): String = b match {
      case FrameBound.UnboundedPreceding => "UNBOUNDED PRECEDING"
      case FrameBound.UnboundedFollowing => "UNBOUNDED FOLLOWING"
      case FrameBound.CurrentRow         => "CURRENT ROW"
      case FrameBound.Preceding(offset)  => s"$offset PRECEDING"
      case FrameBound.Following(offset)  => s"$offset FOLLOWING"
    }
    frame.end match {
      case Some(end) => s"${frame.mode.keyword} BETWEEN ${bound(frame.start)} AND ${bound(end)}"
      case None      => s"${frame.mode.keyword} ${bound(frame.start)}"
    }
  }

  private def tokenizeArrayJoin(arrayJoin: Option[ArrayJoinQuery])(implicit ctx: TokenizeContext): String =
    arrayJoin match {
      case Some(ArrayJoinQuery(columns, _)) if columns.isEmpty => ""
      case Some(ArrayJoinQuery(columns, left))                 =>
        val keyword = if (left) "LEFT ARRAY JOIN" else "ARRAY JOIN"
        // tokenizeColumns, not tokenizeColumnsAliased: ARRAY JOIN introduces the alias rather than referring to one.
        s"$keyword ${tokenizeColumns(columns)}"
      case None => ""
    }

  /**
   * Query-level `SETTINGS`, which is what makes a setting expressible per subquery -- `QuerySettings` can only apply to
   * a whole HTTP request. Values are emitted verbatim, so a string setting has to arrive already quoted.
   */
  private def tokenizeSettings(settings: Seq[(String, String)]): String =
    if (settings.isEmpty) ""
    else settings.map { case (key, value) => s"$key = $value" }.mkString("SETTINGS ", Tokens.Delimiter, "")

  /**
   * `UNION`/`INTERSECT`/`EXCEPT`.
   *
   * A branch that carries set operations of its own is parenthesised, because the chain is otherwise flat and
   * ClickHouse reads a flat chain left to right. `a.unionAll(b.except(c))` would render `a UNION ALL b EXCEPT c`, which
   * groups as `(a UNION ALL b) EXCEPT c` -- and set difference is not associative, so that silently returns something
   * else. The call structure says which grouping was meant, so this preserves it rather than refusing it;
   * [[InternalQuery]]'s own `require` covers the flat case, where nothing says.
   *
   * A trailing `ORDER BY` or `LIMIT` binds to its own branch either way, which is what the per-branch model says.
   */
  private def tokenizeSetOperations(combinations: Seq[SetOperation])(implicit ctx: TokenizeContext): String =
    combinations
      .map { case SetOperation(kind, query) =>
        val branch = toRawSql(query.internalQuery)
        val nested = if (query.internalQuery.combinations.isEmpty) branch else s"($branch)"
        s"${kind.keyword} $nested"
      }
      .mkString(" ")

  private def tokenizeSelect(select: Option[SelectQuery])(implicit ctx: TokenizeContext): String =
    select match {
      case Some(s) =>
        val modifierPart = s.distinctColumns match {
          case Some(distinctColumns) if s.modifier.toUpperCase == "DISTINCT ON" =>
            s"${s.modifier} (${tokenizeColumns(distinctColumns)}) "
          case _ =>
            s.modifier
        }

        val columnsPart = tokenizeColumns(s.columns)
        s"SELECT $modifierPart $columnsPart".trim
      case None => ""
    }

  private def tokenizeFrom(from: Option[FromQuery], withPrefix: Boolean = true)(implicit
      ctx: TokenizeContext
  ): String = {
    require(from != null)
    val fromClause = from match {
      case Some(query: InnerFromQuery)      => s"(${toRawSql(query.innerQuery.internalQuery).trim})"
      case Some(table: TableFromQuery[_])   => table.table.quoted + ctx.tableAlias(table.table)
      case Some(fn: TableFunctionFromQuery) =>
        s"${fn.function.name}(${fn.function.args.map(tokenizeColumn).mkString(Tokens.Delimiter)})"
      case _ => return ""
    }

    val prefix = if (withPrefix) "FROM" else ""
    val alias  = from.flatMap(_.alias.map(s => " AS " + ClickhouseStatement.quoteIdentifier(s))).getOrElse("")
    val asF    = if (from.exists(_.finalized)) " FINAL" else ""
    // After FINAL, not before: `SAMPLE 0.5 FINAL` is a syntax error.
    val sample = from.flatMap(_.sampling).map(tokenizeSample).getOrElse("")
    s"$prefix $fromClause $alias $asF $sample".trim
  }

  private def tokenizeSample(sample: Sample): String = {
    def number(value: Double): String =
      if (value == value.floor && value.abs < 1e15) value.toLong.toString else value.toString
    s"SAMPLE ${number(sample.rate)}" + sample.offset.map(offset => s" OFFSET ${number(offset)}").getOrElse("")
  }

  protected def tokenizeColumn(column: Column)(implicit ctx: TokenizeContext): String = {
    require(column != null)
    column match {
      case EmptyColumn             => ""
      case alias: AliasedColumn[_] =>
        val originalColumnToken = tokenizeColumn(alias.original)
        if (originalColumnToken.isEmpty) alias.quoted else s"$originalColumnToken AS ${alias.quoted}"
      case tuple: TupleColumn[_]     => s"(${tuple.elements.map(tokenizeColumn).mkString(Tokens.Delimiter)})"
      case window: WindowFunction[_] =>
        val over = window.window match {
          case WindowRef.Inline(spec) => s"(${tokenizeWindowSpec(spec)})"
          case WindowRef.Named(name)  => ClickhouseStatement.quoteIdentifier(name)
        }
        s"${tokenizeColumn(window.function)} OVER $over"
      case col: ExpressionColumn[_] => tokenizeExpressionColumn(col)
      case col: Column              => col.quoted
    }
  }

  private def tokenizeExpressionColumn(inCol: ExpressionColumn[_])(implicit ctx: TokenizeContext): String =
    inCol match {
      case agg: AggregateFunction[_]               => tokenizeAggregateFunction(agg)
      case col: ArithmeticFunctionCol[_]           => tokenizeArithmeticFunctionColumn(col)
      case col: ArithmeticFunctionOp[_]            => tokenizeArithmeticFunctionOperator(col)
      case col: ArrayFunction                      => tokenizeArrayFunction(col)
      case col: BitFunction                        => tokenizeBitFunction(col)
      case col: ComparisonColumn                   => tokenizeComparisonColumn(col)
      case col: DateTimeFunctionCol[_]             => tokenizeDateTimeColumn(col)
      case col: DateTimeConst[_]                   => tokenizeDateTimeConst(col)
      case col: DictionaryFuncColumn[_]            => tokenizeDictionaryFunction(col)
      case col: DistanceFunction                   => tokenizeDistanceFunction(col)
      case col: EmptyFunction[_]                   => tokenizeEmptyCol(col)
      case col: EncodingFunction[_]                => tokenizeEncodingFunction(col)
      case col: HashFunction                       => tokenizeHashFunction(col)
      case col: HigherOrderFunction[_, _, _, _, _] => tokenizeHigherOrderFunction(col)
      case col: IPFunction[_]                      => tokenizeIPFunction(col)
      case col: InFunction                         => tokenizeInFunction(col)
      case col: JsonFunction[_]                    => tokenizeJsonFunction(col)
      case col: LogicalFunction                    => tokenizeLogicalFunction(col)
      case col: MathFuncColumn                     => tokenizeMathematicalFunction(col)
      case col: MiscellaneousFunction              => tokenizeMiscellaneousFunction(col)
      case col: NullableFunction                   => tokenizeNullableFunction(col)
      case col: RandomFunction                     => tokenizeRandomFunction(col)
      case col: RoundingFunction                   => tokenizeRoundingFunction(col)
      case col: SplitMergeFunction[_]              => tokenizeSplitMergeFunction(col)
      case col: StringFunctionCol[_]               => tokenizeStringCol(col)
      case col: StringSearchFunc[_]                => tokenizeStringSearchFunction(col)
      case col: TypeCastColumn[_]                  => tokenizeTypeCastColumn(col)
      case col: URLFunction[_]                     => tokenizeURLFunction(col)
      case col: WindowOnlyFunctionCol[_]           => tokenizeWindowOnlyFunction(col)
      case All()                                   => "*"
      case RawColumn(rawSql)                       => rawSql
      case Conditional(cases, default, multiIf)    =>
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
      case a @ _       =>
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
      case TotalDuration                      => s"${interval.start.toInstant.toEpochMilli}"
      case MultiDuration(1, TimeUnit.Year)    => toDateTime(convert("toStartOfYear"))
      case MultiDuration(1, TimeUnit.Quarter) => toDateTime(convert("toStartOfQuarter"))
      case MultiDuration(1, TimeUnit.Month)   => toDateTime(convert("toStartOfMonth"))
      case MultiDuration(1, TimeUnit.Week)    => toDateTime(convert("toMonday"))
      case MultiDuration(1, TimeUnit.Day)     => convert("toStartOfDay")
      case MultiDuration(1, TimeUnit.Hour)    => convert("toStartOfHour")
      case MultiDuration(1, TimeUnit.Minute)  => convert("toStartOfMinute")
      case MultiDuration(1, TimeUnit.Second)  => toDateTime(s"$column / 1000")
      case MultiDuration(nth, TimeUnit.Year)  =>
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
      case unsupported => throw new IllegalArgumentException(s"Unsupported interval: $unsupported")
    }
  }

  // TODO this is a fallback to find a similar timezone when the provided interval does not have a set timezone id.
  // We should be able to disable this from the config and fail fast if we cannot determine the timezone for timeseries
  // (probably default to failing)
  private def determineZoneId(start: ZonedDateTime): String = {
    val zone      = start.getZone
    val available = ZoneId.getAvailableZoneIds.asScala
    if (available.contains(zone.getId)) zone.getId
    else {
      // Sorted, because eight Etc/ zones sit at offset zero and getAvailableZoneIds is unordered -- without this the
      // emitted timezone would vary between runs for a UTC interval.
      val instant = start.toInstant
      available.toSeq.sorted
        .find(id => id.startsWith("Etc/") && ZoneId.of(id).getRules.getOffset(instant) == start.getOffset)
        .getOrElse(throw new IllegalArgumentException(s"Could not determine the zone from source $zone"))
    }
  }

  //  Table joins are tokenized as select * because of https://github.com/yandex/ClickHouse/issues/635
  /**
   * The whole `JOIN` chain, plus the left table's alias, which only exists because a join needs something to qualify
   * its keys with.
   *
   * Every join in the chain keys off the *first* table: the left alias is claimed once and reused, while each join
   * takes its own number for its right alias, giving `L1 ... R1 ... R2`. A join that has to key off an earlier join's
   * right-hand side cannot be spelled here, because a [[com.crobox.clickhouse.dsl.JoinCondition]] names columns and not
   * the table they came from -- nest it in a subquery instead.
   */
  private def tokenizeJoins(
      select: Option[SelectQuery],
      from: Option[FromQuery],
      joins: Seq[JoinQuery],
      arrayJoin: String
  )(implicit
      ctx: TokenizeContext
  ): String =
    if (joins.isEmpty) ""
    else {
      // Claimed up front and held: tokenizing a right-hand side below advances the counter for every join nested in
      // it, so reading it back afterwards would number this join by its subtree and collide with the innermost join.
      val leftNr = ctx.nextJoinNumber()

      val leftAlias =
        if (from.flatMap(_.alias).isEmpty) s"AS ${ctx.leftAlias(from.flatMap(_.alias), leftNr)}" else ""

      val chain = joins.zipWithIndex.map { case (query, index) =>
        // The first join shares the left side's number; the rest claim their own so their right aliases stay distinct.
        val rightNr = if (index == 0) leftNr else ctx.nextJoinNumber()

        // we always need to provide an alias to the RIGHT side
        val right = query.other match {
          case table: TableFromQuery[_] => s"(SELECT * ${tokenizeFrom(Some(table))})"
          // Wrapped for the same reason a table is: a bare join right-hand side contributes no columns to `SELECT *`.
          case fn: TableFunctionFromQuery => s"(SELECT * ${tokenizeFrom(Some(fn))})"
          case query: InnerFromQuery      => tokenizeFrom(Some(query), withPrefix = false)
        }

        Seq(
          if (query.global) "GLOBAL" else "",
          tokenizeJoinType(query.joinType),
          right,
          s"AS ${ctx.rightAlias(query.other.alias, rightNr)}",
          tokenizeJoinKeys(select, from.get, query, leftNr, rightNr)
        )
      }

      // ARRAY JOIN has to land between the left table's alias and the first JOIN keyword.
      (Seq(leftAlias, arrayJoin) ++ chain.flatten).filter(_.nonEmpty).mkString(" ")
    }

  private def tokenizeJoinKeys(
      select: Option[SelectQuery],
      from: FromQuery,
      query: JoinQuery,
      leftNr: Int,
      rightNr: Int
  )(implicit
      ctx: TokenizeContext
  ): String = {

    val using = query.using.filterNot {
      case EmptyColumn => true
      case _           => false
    }
    query.joinType match {
      case CrossJoin | PasteJoin =>
        val name = if (query.joinType == PasteJoin) "PasteJoin" else "CrossJoin"
        require(`using`.isEmpty, s"When using $name, no using columns should be provided")
        require(query.on.isEmpty, s"When using $name, no on conditions should be provided")
        ""
      case _ =>
        require(`using`.nonEmpty || query.on.nonEmpty, s"No USING or ON provided for joinType: ${query.joinType}")
        require(!(`using`.nonEmpty && query.on.nonEmpty), s"Both USING and ON provided for joinType: ${query.joinType}")

        if (`using`.nonEmpty) {
          // A USING key has to resolve against the left *table expression*. Where the left side is a table and the key
          // exists only as a SELECT-list alias, ClickHouse's analyzer -- the default since 24.3 -- rejects it:
          //   SELECT shield_id AS item_id FROM t AS L1 JOIN (...) AS R1 USING item_id
          //   Code 47, UNKNOWN_IDENTIFIER: using identifier 'item_id' cannot be resolved from left table expression
          // The old analyzer resolved such an identifier from the projection. Rather than depend on the
          // analyzer_compatibility_join_using_top_level_identifier shim, express it as ON with the alias resolved back
          // to the underlying column, which both analyzers accept.
          //
          // This holds whether the left side is a table or a subquery -- an alias in this query's own projection is
          // not part of its FROM's table expression in either case. An alias defined *inside* the left subquery is one
          // of its output columns and needs no rewrite, and correctly does not match here.
          if (`using`.exists(key => underlyingLeftColumn(select, key).isDefined)) {
            "ON " + `using`
              .map { key =>
                val left = underlyingLeftColumn(select, key).getOrElse(key.name)
                s"${ctx.leftAlias(from.alias, leftNr)}.$left = ${ctx.rightAlias(query.other.alias, rightNr)}.${key.name}"
              }
              .mkString(" AND ")
          } else if (`using`.size == 1) s"USING ${using.head.name}"
          else s"USING (${using.map(_.name).mkString(Tokens.Delimiter)})"
        } else if (query.on.nonEmpty) {
          // TOKENIZE ON. If the fromClause is a TABLE, we need to check on aliases!
          "ON " + query.on
            .map { cond =>
              val left = underlyingLeftColumn(select, cond.left).getOrElse(cond.left.name)
              s"${ctx.leftAlias(from.alias, leftNr)}.$left ${cond.operator} ${ctx.rightAlias(query.other.alias, rightNr)}.${cond.right.name}"
            }
            .mkString(" AND ")
        } else ""
    }
  }

  /**
   * The column a join key actually refers to on the left side, when the key names an alias introduced by *this* query's
   * SELECT list rather than something the left table expression exposes.
   *
   * Deliberately independent of whether the left side is a table or a subquery. An alias in this query's projection is
   * never part of its own FROM's table expression either way -- for
   * `SELECT shield_id AS item_id FROM (SELECT shield_id FROM t) AS L1`, the subquery outputs `shield_id`, so `item_id`
   * is no more resolvable there than it would be over a bare table. Only an alias defined *inside* the left subquery
   * becomes one of its output columns, and that one does not appear in this select list, so it correctly yields `None`.
   *
   * `None` means no rewrite is needed and USING (or a bare `L.key`) is right as-is.
   */
  private def underlyingLeftColumn(select: Option[SelectQuery], joinKey: Column): Option[String] =
    select
      .map(_.columns)
      .getOrElse(Seq.empty)
      .collectFirst {
        case aliased: AliasedColumn[_] if aliased.alias == joinKey.name && aliased.original.name != joinKey.name =>
          aliased.original.name
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
      case PasteJoin      => "PASTE JOIN"

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

  private def tokenizeFiltering(maybeCondition: Option[TableColumn[Boolean]], keyword: String)(implicit
      ctx: TokenizeContext
  ): String =
    maybeCondition match {
      case None            => ""
      case Some(condition) =>
        // s"$keyword ${tokenizeColumn(condition)}"
        s"$keyword ${removeSurroundingBrackets(tokenizeColumn(condition).trim)}"
    }

  private def tokenizeGroupBy(groupBy: Option[GroupByQuery])(implicit ctx: TokenizeContext): String = {
    val groupByColumns = groupBy match {
      case Some(GroupByQuery(usingColumns, _, _)) if usingColumns.nonEmpty =>
        Option(tokenizeGroupByKeys(usingColumns)).filter(_.nonEmpty).map(keys => s"GROUP BY $keys")
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

  private def tokenizeOrderBy(orderBy: Seq[OrderingColumn])(implicit ctx: TokenizeContext): String =
    orderBy.toList match {
      case Nil | null => ""
      case _          => s"ORDER BY ${tokenizeTuplesAliased(orderBy)}"
    }

  /** `INTERPOLATE` with no entries is the bare form, which interpolates every applicable column. */
  private def tokenizeInterpolate(interpolate: Option[Interpolate])(implicit ctx: TokenizeContext): String =
    interpolate match {
      case None                                          => ""
      case Some(Interpolate(columns)) if columns.isEmpty => "INTERPOLATE"
      case Some(Interpolate(columns))                    =>
        val entries = columns.map { case InterpolateColumn(column, expression) =>
          val target = aliasOrName(column)
          expression.map(expr => s"$target AS ${tokenizeColumn(expr)}").getOrElse(target)
        }
        s"INTERPOLATE (${entries.mkString(Tokens.Delimiter)})"
    }

  private def tokenizeWithFill(fill: WithFill)(implicit ctx: TokenizeContext): String = {
    val parts = Seq(
      fill.from.map(c => s"FROM ${tokenizeColumn(c)}"),
      fill.to.map(c => s"TO ${tokenizeColumn(c)}"),
      fill.step.map(c => s"STEP ${tokenizeColumn(c)}"),
      fill.staleness.map(c => s"STALENESS ${tokenizeColumn(c)}")
    ).flatten
    if (parts.isEmpty) "WITH FILL" else s"WITH FILL ${parts.mkString(" ")}"
  }

  /**
   * `LIMIT`, and `OFFSET` on its own when there is no size to limit to.
   *
   * `WITH TIES` only parses at the very end of the clause, and only after the `LIMIT offset, size` spelling -- both
   * `LIMIT size WITH TIES OFFSET offset` and `LIMIT size OFFSET offset WITH TIES` are syntax errors.
   */
  private def tokenizeLimit(limit: Option[Limit]): String =
    limit match {
      case None                                      => ""
      case Some(Limit(None, offset, _))              => s"OFFSET $offset"
      case Some(Limit(Some(size), offset, withTies)) =>
        s"LIMIT $offset, $size" + (if (withTies) " WITH TIES" else "")
    }

  private def tokenizeLimitBy(limitBy: Option[LimitBy])(implicit ctx: TokenizeContext): String =
    limitBy match {
      case None                                      => ""
      case Some(LimitBy(limit, offset, expressions)) =>
        val expr = expressions.map(tokenizeColumn).mkString(", ")
        if (offset > 0) {
          s"LIMIT $offset, $limit BY $expr"
        } else {
          s"LIMIT $limit BY $expr"
        }
    }

  /**
   * `GROUP BY` keys.
   *
   * An alias is referred to by name, since the projection already defines it, but anything else has to render as
   * itself. `Column.name` for an expression is its *argument's* name, so naming the key here grouped `toStartOfDay(ts)`
   * by the raw `ts` -- a query that succeeds and aggregates over the wrong buckets -- and rendered an arithmetic
   * expression as `NULL`, which the server rejects outright.
   */
  private def tokenizeGroupByKeys(columns: Seq[Column])(implicit ctx: TokenizeContext): String =
    columns
      .map {
        // The alias alone. tokenizeColumn would render `<expression> AS d`, which is a projection, not a key.
        case alias: AliasedColumn[_] => alias.quoted
        case col                     => tokenizeColumn(col)
      }
      .filter(_.nonEmpty)
      .mkString(Tokens.Delimiter)

  private def tokenizeTuplesAliased(columns: Seq[OrderingColumn])(implicit ctx: TokenizeContext): String =
    columns
      // ClickHouse's grammar fixes this order and rejects any other: NULLS before COLLATE, and WITH FILL last.
      .map { case OrderingColumn(column, dir, fill, nulls, collate) =>
        Seq(
          Option(tokenizeColumn(column)),
          Option(direction(dir)),
          nulls.map(_.keyword),
          // A locale name, so a quoted string literal rather than an identifier -- `escape` does the escaping but not
          // the quoting.
          collate.map(locale => s"COLLATE '${ClickhouseStatement.escape(locale)}'"),
          fill.map(tokenizeWithFill)
        ).flatten.mkString(" ")
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
