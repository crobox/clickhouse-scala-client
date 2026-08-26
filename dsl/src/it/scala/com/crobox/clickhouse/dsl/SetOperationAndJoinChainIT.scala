package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec

import java.util.UUID

/**
 * Behavioural cover for the set operations and for a join chain. The validation harness proves the server accepts what
 * these render; it cannot tell `UNION ALL` from `UNION DISTINCT`, or a chain that joins the right tables from one that
 * happens to parse.
 */
class SetOperationAndJoinChainIT extends DslITSpec {

  private val shared    = UUID.randomUUID()
  private val onlyTwo   = UUID.randomUUID()
  private val onlyThree = UUID.randomUUID()

  // column_3 carries which row it is, so a join chain that keeps the wrong row shows up as the wrong value rather
  // than only as the wrong count.
  override val table2Entries: Seq[Table2Entry] = Seq(
    Table2Entry(shared, randomString, 1, "shared", None),
    Table2Entry(onlyTwo, randomString, 1, "only-two", None)
  )

  override val table3Entries: Seq[Table3Entry] =
    Seq(shared, onlyThree).map(id => Table3Entry(id, 1, None, randomString, randomString))

  private def ids(query: OperationalQuery): Seq[String] =
    queryExecutor.executeRows(query).futureValue.rows.map(_.requiredByName[String]("item_id"))

  private val fromTwo   = select(itemId) from TwoTestTable
  private val fromThree = select(itemId) from ThreeTestTable

  "UNION ALL" should "concatenate the branches" in {
    ids(fromTwo.unionAll(fromThree)) should contain theSameElementsAs
    Seq(shared, onlyTwo, shared, onlyThree).map(_.toString)
  }

  "UNION DISTINCT" should "deduplicate across the branches" in {
    ids(fromTwo.unionDistinct(fromThree)) should contain theSameElementsAs
    Seq(shared, onlyTwo, onlyThree).map(_.toString)
  }

  "INTERSECT" should "keep only the rows both branches have" in {
    ids(fromTwo.intersect(fromThree)) shouldBe Seq(shared.toString)
  }

  "EXCEPT" should "keep the rows the second branch does not have" in {
    ids(fromTwo.except(fromThree)) shouldBe Seq(onlyTwo.toString)
  }

  private def labels(query: OperationalQuery): Seq[String] =
    queryExecutor.executeRows(query).futureValue.rows.map(_.requiredByName[String]("column_3"))

  "A chain of joins" should "keep only the rows every table in the chain has" in {
    val query = select(col3)
      .from(TwoTestTable)
      .join(JoinQuery.InnerJoin, ThreeTestTable)
      .on(itemId)
      .join(JoinQuery.InnerJoin, ThreeTestTable)
      .on(itemId)
    // onlyTwo has no match in the dimension, so an inner chain drops it and leaves the shared row alone.
    labels(query) shouldBe Seq("shared")
  }

  it should "key every join off the first table rather than off the previous join" in {
    // The second join's ON reads L1.item_id. Were it reading R1's, joining a table that shares no key with the
    // dimension would produce nothing here.
    val query = select(col3)
      .from(TwoTestTable)
      .join(JoinQuery.AllLeftJoin, ThreeTestTable)
      .on(itemId)
      .join(JoinQuery.AllLeftJoin, ThreeTestTable)
      .on(itemId)
    labels(query) should contain theSameElementsAs Seq("shared", "only-two")
  }

  "ORDER BY ... NULLS" should "put the nulls at the end it names" in {
    // The sort key is NULL for the "shared" row and non-NULL for the other, so the two orderings differ only in
    // which end that row lands at -- the direction alone cannot account for the flip.
    def ordered(nulls: NullsOrder): Seq[String] = {
      val query = select(col3, nullIf(col3, const("shared")) as "n") from TwoTestTable
      labels(query.orderByColumns(OrderingColumn(ref[String]("n"), ASC, nulls = Option(nulls))))
    }
    ordered(NullsOrder.NullsFirst).head shouldBe "shared"
    ordered(NullsOrder.NullsLast).last shouldBe "shared"
  }

  "OFFSET without a limit" should "skip rows and keep the rest" in {
    ids(select(itemId) from TwoTestTable orderBy itemId offset 1) should have size 1
  }

  "LIMIT WITH TIES" should "return more rows than the limit when the last value ties" in {
    // Both rows share column_2 = 1, so a limit of one row extends to both.
    val query = select(itemId, col2) from TwoTestTable orderBy col2 limitWithTies 1
    queryExecutor.executeRows(query).futureValue.rows should have size 2
  }
}
