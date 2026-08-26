package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

import java.util.UUID

/**
 * Behavioural cover for the window-only functions. `SqlValidationITSpec` already proves the server accepts what these
 * render, which does not catch an argument order that resolves either way round -- `nth_value(offset, column)` type
 * checks and resolves as happily as the right way round. These run the query and read the values back.
 */
class WindowFunctionsIT extends DslITSpec {

  // Deliberately inserted out of order, so an ordering the query does not ask for cannot produce the expected answer.
  override val table2Entries: Seq[Table2Entry] =
    Seq(30, 10, 20).map(value => Table2Entry(UUID.randomUUID(), randomString, value, randomString, None))

  private val byCol2 = WindowSpec(orderBy = Seq(col2))

  private def framed(start: FrameBound, end: FrameBound): WindowSpec =
    byCol2.copy(frame = Option(WindowFrame(FrameMode.Rows, start, Option(end))))

  /** Runs `column` over the fixture and reads it back in window order. */
  private def ints(column: TableColumn[_]): Seq[Int] =
    queryExecutor
      .executeRows(select(col2, column as "result") from TwoTestTable orderBy col2)
      .futureValue
      .rows
      .map(_.requiredByName[Int]("result"))

  "row_number" should "number the rows in window order" in {
    ints(rowNumber().over(byCol2)) shouldBe Seq(1, 2, 3)
  }

  "rank and dense_rank" should "agree when there are no ties" in {
    ints(rank().over(byCol2)) shouldBe Seq(1, 2, 3)
    ints(denseRank().over(byCol2)) shouldBe Seq(1, 2, 3)
  }

  "ntile" should "spread the rows over the buckets" in {
    ints(ntile(3).over(byCol2)) shouldBe Seq(1, 2, 3)
  }

  // The default is 0 rather than a sentinel like -1 because column_2 is UInt32 and a negative literal widens the
  // supertype to Int64, which the server rejects outright.
  "lagInFrame" should "read the previous row, falling back on the default" in {
    ints(
      lagInFrame(col2, Option(const(1L)), Option(const(0)))
        .over(framed(FrameBound.Preceding(1), FrameBound.CurrentRow))
    ) shouldBe Seq(0, 10, 20)
  }

  it should "reject a default that widens the column's type" in {
    val query = select(
      lagInFrame(col2, Option(const(1L)), Option(const(-1)))
        .over(framed(FrameBound.Preceding(1), FrameBound.CurrentRow)) as "result"
    ) from TwoTestTable
    val failure = queryExecutor.executeRows(query).failed.futureValue
    failure.getMessage should include("is not the same as the argument type")
  }

  "leadInFrame" should "read the next row, falling back on the default" in {
    ints(
      leadInFrame(col2, Option(const(1L)), Option(const(0)))
        .over(framed(FrameBound.CurrentRow, FrameBound.Following(1)))
    ) shouldBe Seq(20, 30, 0)
  }

  // The one that would pass a rendering check with its arguments the wrong way round.
  "nth_value" should "read the nth row of the frame, counting from one" in {
    ints(
      nthValue(col2, 2).over(framed(FrameBound.UnboundedPreceding, FrameBound.UnboundedFollowing))
    ) shouldBe Seq(20, 20, 20)
  }

  "argMin and argMax" should "return the argument from the row holding the extreme value" in {
    val row = queryExecutor
      .executeRows(select(argMin(col2, col2) as "low", argMax(col2, col2) as "high") from TwoTestTable)
      .futureValue
      .rows
      .head
    row.requiredByName[Int]("low") shouldBe 10
    row.requiredByName[Int]("high") shouldBe 30
  }

  "count" should "differ from count DISTINCT only when there are duplicates" in {
    val row = queryExecutor
      .executeRows(select(count(col2) as "all", countDistinct(col2) as "distinct") from TwoTestTable)
      .futureValue
      .rows
      .head
    row.requiredByName[Int]("all") shouldBe 3
    row.requiredByName[Int]("distinct") shouldBe 3
  }
}
