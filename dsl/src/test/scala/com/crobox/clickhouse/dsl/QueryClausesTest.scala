package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

/**
 * Clause-level rendering: that each clause appears at all, and in the position ClickHouse's grammar requires. Position
 * is the part worth asserting -- a clause emitted in the wrong slot still type-checks and still reads plausibly, and
 * `SAMPLE 0.5 FINAL` is a syntax error where `FINAL SAMPLE 0.5` is not.
 *
 * https://clickhouse.com/docs/sql-reference/statements/select
 */
class QueryClausesTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  "ARRAY JOIN" should "unfold a single column" in {
    sql(select(shieldId).from(OneTestTable).withArrayJoin(numbers)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} ARRAY JOIN numbers"
    )
  }

  it should "render LEFT ARRAY JOIN" in {
    sql(select(shieldId).from(OneTestTable).withLeftArrayJoin(numbers)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} LEFT ARRAY JOIN numbers"
    )
  }

  it should "carry an alias, which is how the unfolded column gets named" in {
    sql(select(shieldId).from(OneTestTable).withArrayJoin(numbers as "n")) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} ARRAY JOIN numbers AS n"
    )
  }

  it should "accumulate columns across calls rather than replacing them" in {
    val query = select(shieldId).from(OneTestTable).withArrayJoin(numbers as "n").withArrayJoin(numbers as "m")
    sql(query) should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} ARRAY JOIN numbers AS n, numbers AS m")
  }

  it should "sit between FROM and WHERE" in {
    val query = select(shieldId).from(OneTestTable).withArrayJoin(numbers).where(shieldId isEq "a")
    sql(query) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} ARRAY JOIN numbers WHERE shield_id = 'a'"
    )
  }

  // Uses a real Array column, so this asserts a query the server also accepts -- see the matching entry in
  // SqlValidationITSpec. The alias has to land on the FROM table before ARRAY JOIN, not after it.
  it should "sit after the left table's alias and before JOIN" in {
    val query = select(shieldId as itemId)
      .from(OneTestTable)
      .withArrayJoin(numbers as "n")
      .join(JoinQuery.InnerJoin, TwoTestTable) using itemId
    sql(query) should matchSQL(
      s"SELECT shield_id AS item_id FROM ${OneTestTable.quoted} AS L1 ARRAY JOIN numbers AS n " +
        s"INNER JOIN (SELECT * FROM ${TwoTestTable.quoted}) AS R1 ON L1.shield_id = R1.item_id"
    )
  }

  it should "render nothing when no columns were given" in {
    sql(select(shieldId).from(OneTestTable).withArrayJoin()) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted}"
    )
  }

  "SAMPLE" should "render a fractional rate" in {
    sql(select(shieldId).from(OneTestTable).sample(0.1)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} SAMPLE 0.1"
    )
  }

  it should "render a row count without a spurious decimal point" in {
    sql(select(shieldId).from(OneTestTable).sample(1000)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} SAMPLE 1000"
    )
  }

  it should "render an offset" in {
    sql(select(shieldId).from(OneTestTable).sample(0.1, Option(0.5))) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} SAMPLE 0.1 OFFSET 0.5"
    )
  }

  // `SAMPLE 0.5 FINAL` is rejected by the server; only this order parses.
  it should "come after FINAL, not before" in {
    sql(select(shieldId).from(OneTestTable).asFinal.sample(0.1)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} FINAL SAMPLE 0.1"
    )
  }

  it should "be refused on a subquery, which has no sampling key" in {
    an[IllegalArgumentException] should be thrownBy
    select(shieldId).from(select(shieldId).from(OneTestTable)).sample(0.1)
  }

  "SETTINGS" should "render a single setting" in {
    sql(select(shieldId).from(OneTestTable).settings("max_threads" -> "1")) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} SETTINGS max_threads = 1"
    )
  }

  it should "render several in the order given" in {
    val query = select(shieldId).from(OneTestTable).settings("max_threads" -> "1", "max_block_size" -> "100")
    sql(query) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} SETTINGS max_threads = 1, max_block_size = 100"
    )
  }

  it should "come after LIMIT" in {
    val query = select(shieldId).from(OneTestTable).limit(Option(Limit(10))).settings("max_threads" -> "1")
    sql(query) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 0, 10 SETTINGS max_threads = 1"
    )
  }

  // The clause has to land inside the query it belongs to, not after the union of both.
  it should "come before UNION ALL" in {
    val query = select(shieldId)
      .from(OneTestTable)
      .settings("max_threads" -> "1")
      .unionAll(select(shieldId).from(OneTestTable))
    sql(query) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} SETTINGS max_threads = 1 " +
        s"UNION ALL SELECT shield_id FROM ${OneTestTable.quoted}"
    )
  }

  it should "come before FORMAT" in {
    val query = select(shieldId).from(OneTestTable).settings("max_threads" -> "1")
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} SETTINGS max_threads = 1 FORMAT JSON"
    )
  }

  "PASTE JOIN" should "render without join keys" in {
    val query = select(itemId).from(TwoTestTable).join(JoinQuery.PasteJoin, ThreeTestTable)
    sql(query) should matchSQL(
      s"SELECT item_id FROM ${TwoTestTable.quoted} AS L1 PASTE JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R1"
    )
  }

  it should "refuse USING, which it concatenates by position instead of" in {
    val query = select(itemId).from(TwoTestTable).join(JoinQuery.PasteJoin, ThreeTestTable) using itemId
    an[IllegalArgumentException] should be thrownBy sql(query)
  }

  it should "refuse ON" in {
    val query = select(itemId).from(TwoTestTable).join(JoinQuery.PasteJoin, ThreeTestTable) on itemId
    an[IllegalArgumentException] should be thrownBy sql(query)
  }

  "LIMIT" should "render offset and size" in {
    sql(select(shieldId).from(OneTestTable).limit(10, 5)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 5, 10"
    )
  }

  it should "render a size on its own" in {
    sql(select(shieldId).from(OneTestTable).limit(10)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 0, 10"
    )
  }

  // WITH TIES parses only at the end of the clause, and only after `LIMIT offset, size` -- neither
  // `LIMIT size WITH TIES OFFSET offset` nor `LIMIT size OFFSET offset WITH TIES` is accepted.
  it should "render WITH TIES last, alone and with an offset" in {
    sql(select(shieldId).from(OneTestTable).limitWithTies(10)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 0, 10 WITH TIES"
    )
    sql(select(shieldId).from(OneTestTable).limitWithTies(10, 5)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 5, 10 WITH TIES"
    )
  }

  it should "refuse WITH TIES with no size to extend" in {
    an[IllegalArgumentException] should be thrownBy Limit(None, 5, withTies = true)
  }

  "OFFSET" should "render on its own, with no LIMIT" in {
    sql(select(shieldId).from(OneTestTable).offset(5)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} OFFSET 5"
    )
  }

  it should "keep a size that was already set" in {
    sql(select(shieldId).from(OneTestTable).limit(10).offset(5)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 5, 10"
    )
  }

  // ClickHouse fixes this order and rejects any other: NULLS after the direction, COLLATE after NULLS, WITH FILL last.
  "ORDER BY" should "render NULLS FIRST and NULLS LAST" in {
    sql(
      select(shieldId)
        .from(OneTestTable)
        .orderByColumns(OrderingColumn(shieldId, DESC, nulls = Option(NullsOrder.NullsFirst)))
    ) should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} ORDER BY shield_id DESC NULLS FIRST")
    sql(
      select(shieldId)
        .from(OneTestTable)
        .orderByColumns(OrderingColumn(shieldId, ASC, nulls = Option(NullsOrder.NullsLast)))
    ) should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} ORDER BY shield_id ASC NULLS LAST")
  }

  it should "render COLLATE, quoted as a literal" in {
    sql(select(shieldId).from(OneTestTable).orderByColumns(OrderingColumn(shieldId, collate = Option("en")))) should
    matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} ORDER BY shield_id ASC COLLATE 'en'")
  }

  it should "render direction, NULLS, COLLATE and WITH FILL in the order the grammar requires" in {
    sql(
      select(shieldId)
        .from(OneTestTable)
        .orderByColumns(
          OrderingColumn(
            shieldId,
            DESC,
            fill = Option(WithFill(step = Option(const(1)))),
            nulls = Option(NullsOrder.NullsLast),
            collate = Option("en")
          )
        )
    ) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} " +
        s"ORDER BY shield_id DESC NULLS LAST COLLATE 'en' WITH FILL STEP 1"
    )
  }
}
