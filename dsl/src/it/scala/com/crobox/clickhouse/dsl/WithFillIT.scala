package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

/**
 * `WITH FILL` end to end. The point of the clause is rows that are not in the table, so these assert the gaps were
 * actually filled rather than that the SQL merely parsed.
 */
class WithFillIT extends DslITSpec {

  private case class Point(value: Int, tag: String)
  private implicit val pointFormat: RootJsonFormat[Point] =
    jsonFormat[Int, String, Point](Point.apply, "column_2", "column_3")

  // column_2 is a UInt32, so the fill bounds are plain numbers rather than dates.
  override val table2Entries: Seq[Table2Entry] =
    Seq(Table2Entry(randomUUID, "a", 1, "x", None), Table2Entry(randomUUID, "b", 4, "y", None))

  private val ordered = select(col2, col3).from(TwoTestTable)

  it should "invent the rows between the ones present" in {
    val filled = ordered
      .orderByColumns(
        OrderingColumn(
          col2,
          ASC,
          Option(WithFill(from = Option(const(1)), to = Option(const(5)), step = Option(const(1))))
        )
      )
    val values = queryExecutor.execute[Point](filled).futureValue.rows.map(_.value)
    // 1 and 4 exist; 2, 3 are invented, and TO is exclusive so 5 is not.
    values should contain theSameElementsInOrderAs Seq(1, 2, 3, 4)
  }

  it should "leave the filled rows' other columns empty without INTERPOLATE" in {
    val filled = ordered
      .orderByColumns(
        OrderingColumn(
          col2,
          ASC,
          Option(WithFill(from = Option(const(1)), to = Option(const(4)), step = Option(const(1))))
        )
      )
    val rows = queryExecutor.execute[Point](filled).futureValue.rows
    rows.find(_.value == 2).map(_.tag) shouldBe Some("")
  }

  it should "carry the previous value into the filled rows with INTERPOLATE" in {
    val filled = ordered
      .orderByColumns(
        OrderingColumn(
          col2,
          ASC,
          Option(WithFill(from = Option(const(1)), to = Option(const(4)), step = Option(const(1))))
        )
      )
      .interpolate(InterpolateColumn(col3))
    val rows = queryExecutor.execute[Point](filled).futureValue.rows
    rows.find(_.value == 2).map(_.tag) shouldBe Some("x")
  }

  it should "step by more than one" in {
    val filled = ordered
      .orderByColumns(
        OrderingColumn(
          col2,
          ASC,
          Option(WithFill(from = Option(const(1)), to = Option(const(6)), step = Option(const(2))))
        )
      )
    queryExecutor.execute[Point](filled).futureValue.rows.map(_.value) should contain allOf (1, 3, 5)
  }
}
