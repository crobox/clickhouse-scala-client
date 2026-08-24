package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

import scala.util.{Failure, Success}

/**
 * Guards the field inventory of [[InternalQuery]] rather than any SQL it produces.
 *
 * Adding a clause means adding a field, and only one of the places that field has to be handled is checked by the
 * compiler: `toRawSql` destructures positionally, so it stops building. `:+>` and `+` both name their arguments, so an
 * unmentioned field just takes its default -- a merge quietly discards it, and a genuine conflict quietly passes. That
 * is how `unionAll` came to be dropped from every merge for as long as it existed.
 *
 * [[FieldCount]] is the tripwire that sends the author here; the tests below are what actually verify the merge.
 */
class InternalQueryFieldsTest extends DslTestSpec {

  /**
   * Adding a field to [[InternalQuery]] means updating all four of these:
   *   1. `ClickhouseTokenizerModule.toRawSql` -- its positional pattern stops compiling, so this one is automatic
   *   2. `InternalQuery.:+>` -- otherwise the field is silently dropped by every merge
   *   3. `InternalQuery.+` -- otherwise two queries that conflict on it merge without complaint
   *   4. this constant, plus setting the field in [[populated]] and adding it to [[singleFieldQueries]]
   */
  private val FieldCount = 13

  private val condition = col3 isEq "a"

  /** Every field set to something distinguishable from its default. */
  private val populated = InternalQuery(
    select = Option(SelectQuery(Seq(itemId))),
    from = Option(TableFromQuery(TwoTestTable)),
    prewhere = Option(condition),
    where = Option(condition),
    groupBy = Option(GroupByQuery(usingColumns = Seq(itemId))),
    having = Option(condition),
    join = Option(JoinQuery(JoinQuery.InnerJoin, TableFromQuery(ThreeTestTable), `using` = Seq(itemId))),
    arrayJoin = Option(ArrayJoinQuery(Seq(numbers))),
    orderBy = Seq((itemId, DESC)),
    limit = Option(Limit(10, 5)),
    limitBy = Option(LimitBy(1, 0, Seq(itemId))),
    unionAll = Seq(select(itemId).from(OneTestTable)),
    settings = Seq("max_threads" -> "1")
  )

  /** One query per field, carrying only that field, so a missing conflict check shows up as a merge that succeeds. */
  private val singleFieldQueries: Seq[(String, InternalQuery)] = Seq(
    "select"    -> InternalQuery(select = populated.select),
    "from"      -> InternalQuery(from = populated.from),
    "prewhere"  -> InternalQuery(prewhere = populated.prewhere),
    "where"     -> InternalQuery(where = populated.where),
    "groupBy"   -> InternalQuery(groupBy = populated.groupBy),
    "having"    -> InternalQuery(having = populated.having),
    "join"      -> InternalQuery(join = populated.join),
    "arrayJoin" -> InternalQuery(arrayJoin = populated.arrayJoin),
    "orderBy"   -> InternalQuery(orderBy = populated.orderBy),
    "limit"     -> InternalQuery(limit = populated.limit),
    "limitBy"   -> InternalQuery(limitBy = populated.limitBy),
    "unionAll"  -> InternalQuery(unionAll = populated.unionAll),
    "settings"  -> InternalQuery(settings = populated.settings)
  )

  /** Names the fields that differ, because an InternalQuery's `toString` is far too long to diff by eye. */
  private def differingFields(actual: InternalQuery, expected: InternalQuery): Seq[String] =
    expected.productElementNames.toSeq.zipWithIndex.collect {
      case (name, i) if actual.productElement(i) != expected.productElement(i) =>
        s"$name (got ${actual.productElement(i)}, expected ${expected.productElement(i)})"
    }

  it should "not gain a field without every merge path being updated" in {
    withClue(
      "InternalQuery's field count changed. Read the comment on FieldCount: the new field also has to be handled in " +
        ":+> and in +, and set in `populated` and `singleFieldQueries`, or merging will silently drop it.\n"
    ) {
      InternalQuery().productArity shouldBe FieldCount
    }
    singleFieldQueries.map(_._1) should contain theSameElementsAs populated.productElementNames.toSeq
  }

  it should "carry every field through a merge, from either side" in {
    withClue("dropped when merging into an empty query: ") {
      differingFields(populated :+> InternalQuery(), populated) shouldBe empty
    }
    withClue("dropped when an empty query is merged into: ") {
      differingFields(InternalQuery() :+> populated, populated) shouldBe empty
    }
  }

  it should "detect a conflict on any single field" in {
    val unchecked = singleFieldQueries.collect { case (name, query) if (query + query).isSuccess => name }
    withClue(s"these fields merged without complaint despite conflicting, so + is missing a check: $unchecked\n") {
      unchecked shouldBe empty
    }
  }

  it should "merge cleanly when the two sides share no field" in {
    val left  = InternalQuery(select = populated.select, where = populated.where)
    val right = InternalQuery(from = populated.from, limit = populated.limit)
    left + right match {
      case Success(merged) =>
        differingFields(
          merged,
          InternalQuery(
            select = populated.select,
            from = populated.from,
            where = populated.where,
            limit = populated.limit
          )
        ) shouldBe empty
      case Failure(error) => fail(s"disjoint parts should merge, got: ${error.getMessage}")
    }
  }
}
