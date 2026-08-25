package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

/**
 * Asserts that the SQL this DSL emits is SQL ClickHouse actually accepts.
 *
 * The gap this closes: a tokenizer arm is a string template, so a wrong one still type-checks and still passes a
 * hand-written `should matchSQL("...")` assertion as long as the expectation was written to match the bug. The tuple
 * access operator emitted `x.2)` -- an unmatched parenthesis -- for years, because nothing ever rendered it and no
 * expectation existed to contradict. Sending the generated text to ClickHouse's own parser is the only check that
 * cannot be written to agree with a mistake.
 *
 * Two strengths of check, because they need different things from the server:
 *   - [[Semantic]] uses `EXPLAIN QUERY TREE`, which resolves function names, arity and argument types. It needs the new
 *     analyzer, so it is only used from 24.3 onwards and degrades to a syntax check on older servers.
 *   - [[Syntax]] uses `EXPLAIN AST`, which parses and nothing more. This is all that is available for constructs whose
 *     names cannot resolve in a bare test database -- the dictionary functions need a configured dictionary.
 *
 * Neither form executes the query, so constructs may reference tables and columns freely and cost nothing to check.
 */
class SqlValidationITSpec extends DslITSpec {

  sealed trait ValidationLevel

  /** Parse, resolve names, check arity and argument types. */
  case object Semantic extends ValidationLevel

  /** Parse only. */
  case object Syntax extends ValidationLevel

  /**
   * @param ast
   *   for entries in [[astConstructs]], the simple name of the AST case class this exercises -- the coverage test
   *   matches it against the declarations in `dsl/column`, so it must spell one of them exactly. For [[joinShapes]] it
   *   is just a description, since those cover a whole-query shape rather than one node.
   */
  case class Construct(ast: String, query: OperationalQuery, level: ValidationLevel = Semantic)

  private def sem(ast: String, query: OperationalQuery) = Construct(ast, query, Semantic)
  private def syn(ast: String, query: OperationalQuery) = Construct(ast, query, Syntax)

  private val bitConstructs = Seq(
    sem("BitAnd", select(bitAnd(3, 5))),
    sem("BitOr", select(bitOr(3, 5))),
    sem("BitXor", select(bitXor(3, 5))),
    sem("BitNot", select(bitNot(3))),
    sem("BitShiftLeft", select(bitShiftLeft(3, 2))),
    sem("BitShiftRight", select(bitShiftRight(3, 2)))
  )

  private val randomConstructs = Seq(
    sem("Rand", select(rand())),
    sem("Rand64", select(rand64()))
  )

  private val roundingConstructs = Seq(
    sem("Floor", select(floor(1.5, 1))),
    sem("Ceil", select(ceil(1.5, 1))),
    sem("Round", select(round(1.5, 1))),
    sem("RoundToExp2", select(roundToExp2(5))),
    sem("RoundDuration", select(roundDuration(5))),
    sem("RoundAge", select(roundAge(25)))
  )

  private val mathConstructs = Seq(
    sem("E", select(e())),
    sem("Pi", select(pi())),
    sem("Exp", select(exp(1))),
    sem("Log", select(log(1))),
    sem("Exp2", select(exp2(1))),
    sem("Log2", select(log2(1))),
    sem("Exp10", select(exp10(1))),
    sem("Log10", select(log10(1))),
    sem("Sqrt", select(sqrt(4))),
    sem("Cbrt", select(cbrt(8))),
    sem("Erf", select(erf(1))),
    sem("Erfc", select(erfc(1))),
    sem("Lgamma", select(lgamma(1))),
    sem("Tgamma", select(tgamma(1))),
    sem("Sin", select(sin(1))),
    sem("Cos", select(cos(1))),
    sem("Tan", select(tan(1))),
    sem("Asin", select(asin(0.5))),
    sem("Acos", select(acos(0.5))),
    sem("Atan", select(atan(1))),
    sem("Pow", select(pow(2, 3)))
  )

  /**
   * Syntax only: `dictGet*` resolves against the server's configured dictionaries, and a test database has none, so
   * `EXPLAIN QUERY TREE` would report a missing dictionary rather than anything about our rendering.
   */
  private val dictionaryConstructs = Seq(
    syn("DictGetUInt8", select(dictGetUInt8("dict", "attr", 1))),
    syn("DictGetUInt16", select(dictGetUInt16("dict", "attr", 1))),
    syn("DictGetUInt32", select(dictGetUInt32("dict", "attr", 1))),
    syn("DictGetUInt64", select(dictGetUInt64("dict", "attr", 1))),
    syn("DictGetInt8", select(dictGetInt8("dict", "attr", 1))),
    syn("DictGetInt16", select(dictGetInt16("dict", "attr", 1))),
    syn("DictGetInt32", select(dictGetInt32("dict", "attr", 1))),
    syn("DictGetInt64", select(dictGetInt64("dict", "attr", 1))),
    syn("DictGetFloat32", select(dictGetFloat32("dict", "attr", 1))),
    syn("DictGetFloat64", select(dictGetFloat64("dict", "attr", 1))),
    syn("DictGetDate", select(dictGetDate("dict", "attr", 1))),
    syn("DictGetDateTime", select(dictGetDateTime("dict", "attr", 1))),
    syn("DictGetUUID", select(dictGetUUID("dict", "attr", 1))),
    syn("DictGetString", select(dictGetString("dict", "attr", 1))),
    syn("DictIsIn", select(dictIsIn("dict", 1, 2))),
    syn("DictGetHierarchy", select(dictGetHierarchy("dict", 1))),
    syn("DictHas", select(dictHas("dict", 1)))
  )

  /**
   * Regression guards for the tuple/map area. `TupleElement` is the arm that shipped broken; the `*Map` aggregates are
   * the reason it is reachable at all.
   */
  private val tupleAndMapConstructs = Seq(
    sem("SumMap", select(sumMap(numbers, numbers)) from OneTestTable),
    sem("MinMap", select(minMap(numbers, numbers)) from OneTestTable),
    sem("MaxMap", select(maxMap(numbers, numbers)) from OneTestTable),
    sem("TupleElement", select(mapValues(sumMap(numbers, numbers))) from OneTestTable),
    sem("Tuple", select(tupleElement[Int](tuple(1, 2), 1))),
    sem("Array", select(arrayOf(1, 2, 3)))
  )

  /** AST nodes covered above; these are what the coverage ratchet counts. */
  private val astConstructs: Seq[Construct] =
    bitConstructs ++ randomConstructs ++ roundingConstructs ++ mathConstructs ++
      dictionaryConstructs ++ tupleAndMapConstructs

  /**
   * Whole-query shapes rather than individual functions, so they are validated but not counted towards AST coverage.
   *
   * The join shapes are here because of the ClickHouse 24.3 analyzer change: a `USING` key must resolve against the
   * left table expression, so `select(a as b) ... join(t) using b` over a *table* cannot render as `USING b` -- it has
   * to become `ON L.a = R.b`. Over a *subquery* the alias is a real output column and `USING` is still correct. Both
   * directions are pinned here because only a real server can tell them apart.
   */
  private val joinShapes: Seq[Construct] = Seq(
    Construct(
      "join using a projection alias over a table",
      select(shieldId as itemId).from(OneTestTable).join(JoinQuery.InnerJoin, TwoTestTable) using itemId
    ),
    Construct(
      "join using a projection alias over a subquery",
      select(shieldId as itemId)
        .from(select(shieldId).from(OneTestTable))
        .join(JoinQuery.InnerJoin, TwoTestTable) using itemId
    ),
    Construct(
      "join using a real column",
      select(itemId).from(TwoTestTable).join(JoinQuery.InnerJoin, ThreeTestTable) using itemId
    ),
    Construct(
      "join on an explicit condition",
      select(shieldId as itemId).from(OneTestTable).join(JoinQuery.InnerJoin, TwoTestTable) on itemId
    )
  )

  /**
   * Whole-query clause shapes, so they live here rather than in [[astConstructs]] -- the coverage ratchet matches those
   * names against the declarations in `dsl/column`, and a clause has no node there.
   */
  private val clauseShapes: Seq[Construct] = Seq(
    Construct("array join", select(shieldId).from(OneTestTable).withArrayJoin(numbers as "n")),
    Construct("left array join", select(shieldId).from(OneTestTable).withLeftArrayJoin(numbers as "n")),
    Construct(
      "array join alongside a join, where it has to follow the left table's alias",
      select(shieldId as itemId)
        .from(OneTestTable)
        .withArrayJoin(numbers as "n")
        .join(JoinQuery.InnerJoin, TwoTestTable) using itemId
    ),
    Construct("query-level settings", select(shieldId).from(OneTestTable).settings("max_threads" -> "1")),
    Construct(
      "settings before a union",
      select(shieldId).from(OneTestTable).settings("max_threads" -> "1").unionAll(select(shieldId).from(OneTestTable))
    ),
    Construct("paste join", select(itemId).from(TwoTestTable).join(JoinQuery.PasteJoin, ThreeTestTable)),
    // Syntax only: the IT tables are Engine.Memory, and resolving SAMPLE against a table with no sampling key fails
    // with SAMPLING_NOT_SUPPORTED before the analyzer gets to the clause itself.
    Construct(
      "with, naming an expression",
      select(ref[Int]("one")).from(OneTestTable).withCte(WithExpression(const(1), "one"))
    ),
    Construct(
      "with, naming a scalar subquery",
      select(itemId, ref[Long]("total"))
        .from(TwoTestTable)
        .withCte(WithScalarQuery(select(count()).from(TwoTestTable), "total"))
    ),
    Construct(
      "with, declaring a CTE and selecting from it", {
        val recent = WithTable("recent", select(itemId).from(TwoTestTable))
        select(itemId).from(recent).withCte(recent)
      }
    ),
    Construct(
      "with, scoped to a union branch",
      select(ref[Int]("one"))
        .from(OneTestTable)
        .withCte(WithExpression(const(1), "one"))
        .unionAll(select(ref[Int]("two")).from(OneTestTable).withCte(WithExpression(const(2), "two")))
    ),
    Construct(
      "order by with fill",
      select(col2).from(TwoTestTable).orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
    ),
    Construct(
      "order by with fill, bounded",
      select(col2)
        .from(TwoTestTable)
        .orderByColumns(
          OrderingColumn(
            col2,
            ASC,
            Option(WithFill(from = Option(const(1)), to = Option(const(6)), step = Option(const(1))))
          )
        )
    ),
    Construct(
      "order by with fill and staleness, which cannot be combined with FROM",
      select(col2)
        .from(TwoTestTable)
        .orderByColumns(
          OrderingColumn(col2, ASC, Option(WithFill(to = Option(const(6)), staleness = Option(const(3)))))
        )
    ),
    Construct(
      "interpolate, naming a column",
      select(col2, col3)
        .from(TwoTestTable)
        .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
        .interpolate(InterpolateColumn(col3))
    ),
    Construct(
      "interpolate, bare",
      select(col2, col3)
        .from(TwoTestTable)
        .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
        .interpolate()
    ),
    Construct(
      "group by a column an aggregate already covers, which is now projected",
      select(sum(col2)).from(TwoTestTable).groupBy(col2)
    ),
    Construct(
      "order by an expression differing from the projection",
      select(sum(col2)).from(TwoTestTable).orderBy(uniq(col2))
    ),
    Construct("re-aliased column", select((shieldId as "from_pv") as "from_start").from(OneTestTable)),
    Construct(
      "an inner alias referenced from the enclosing query",
      select(ref[String]("from_pv") as "from_start").from(select(shieldId as "from_pv").from(OneTestTable))
    ),
    Construct("sample", select(shieldId).from(OneTestTable).sample(0.1), Syntax),
    Construct("sample with an offset", select(shieldId).from(OneTestTable).sample(0.1, Option(0.5)), Syntax)
  )

  private val constructs: Seq[Construct] = astConstructs ++ joinShapes ++ clauseShapes

  /** `EXPLAIN QUERY TREE` needs the analyzer, the default since 24.3 and so on every supported server. */
  private def explainOf(level: ValidationLevel): String = level match {
    case Semantic => "EXPLAIN QUERY TREE "
    case _        => "EXPLAIN AST "
  }

  it should "emit SQL that ClickHouse parses, for every registered construct" in {
    // One at a time. Firing all of them concurrently overruns the client's connection pool
    // (pekko.http.host-connection-pool.max-open-requests, 32 by default) and every result becomes a pool rejection
    // rather than a verdict on the SQL.
    val failures = constructs.flatMap { construct =>
      val sql = toSql(construct.query.internalQuery, None)
      clickClient
        .query(explainOf(construct.level) + sql)
        .map(_ => None)
        .recover { case error => Some((construct, sql, firstLineOf(error))) }
        .futureValue
    }

    if (failures.nonEmpty) {
      val report = failures
        .map { case (construct, sql, message) =>
          s"  ${construct.ast} (${construct.level})\n    emitted: $sql\n    server : $message"
        }
        .mkString("\n")
      fail(s"${failures.size} of ${constructs.size} constructs emitted SQL ClickHouse rejected:\n$report")
    }
  }

  /**
   * A ratchet, not a gate. 348 AST case classes exist in `dsl/column` and most have no rendering check anywhere; this
   * records how many and refuses to let that number grow, so a new function cannot be added without a check while the
   * backlog is worked off by lowering the baseline.
   */
  it should "not grow the set of AST nodes with no rendering check" in {
    val declared   = declaredAstNames()
    val registered = astConstructs.map(_.ast).toSet

    val unknown = registered.diff(declared)
    withClue(s"registered under names that no longer exist in dsl/column (renamed or misspelled): $unknown\n") {
      unknown shouldBe empty
    }

    val unregistered = declared.diff(registered).toSeq.sorted
    withClue(
      s"${unregistered.size} AST nodes have no entry in this registry. Lower UnregisteredBaseline as you add them.\n" +
        unregistered.mkString(", ") + "\n"
    ) {
      unregistered.size should be <= SqlValidationITSpec.UnregisteredBaseline
    }
  }

  private def firstLineOf(error: Throwable): String =
    Option(error.getMessage).map(_.linesIterator.next().take(200)).getOrElse(error.getClass.getName)

  /** Simple names of every `case class` declared under `dsl/column`, read from source. */
  private def declaredAstNames(): Set[String] = {
    // Walk up to the repo root -- the working directory is the repo root when tests are unforked and the project
    // directory when they are forked, and that is a build setting we should not depend on here.
    val start    = new java.io.File(".").getCanonicalFile
    val repoRoot = Iterator
      .iterate(Option(start))(_.flatMap(d => Option(d.getParentFile)))
      .takeWhile(_.isDefined)
      .flatten
      .find(d => new java.io.File(d, "build.sbt").isFile)
      .getOrElse(fail(s"Could not find the repository root (no build.sbt at or above $start)"))

    val dir = new java.io.File(repoRoot, "dsl/src/main/scala/com/crobox/clickhouse/dsl/column")
    if (!dir.isDirectory) fail(s"Expected DSL column sources at $dir")

    val declaration = """^\s*(?:final\s+)?case class\s+([A-Za-z0-9_]+)""".r
    dir
      .listFiles(new java.io.FilenameFilter {
        override def accept(unused: java.io.File, name: String): Boolean = name.endsWith(".scala")
      })
      .toSeq
      .flatMap { file =>
        val source = scala.io.Source.fromFile(file, "UTF-8")
        try source.getLines().flatMap(declaration.findFirstMatchIn(_).map(_.group(1))).toList
        finally source.close()
      }
      .toSet
  }
}

object SqlValidationITSpec {

  /**
   * How many of the 346 AST nodes in `dsl/column` still have no entry in the registry. Only ever lower this: it exists
   * so that adding a function without a rendering check fails, while the existing backlog is worked off deliberately
   * rather than blocking this harness on writing 347 entries up front.
   */
  val UnregisteredBaseline = 288
}
