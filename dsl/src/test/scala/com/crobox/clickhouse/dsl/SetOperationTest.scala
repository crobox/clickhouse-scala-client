package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

/**
 * `UNION` / `INTERSECT` / `EXCEPT`.
 *
 * The chain renders flat, which only reads left to right while every step shares one precedence level. `INTERSECT`
 * binds tighter than the other two, so mixing it in is refused rather than silently reordered.
 */
class SetOperationTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  private val left  = select(shieldId) from OneTestTable
  private val right = select(itemId) from TwoTestTable

  private def combined(keyword: String) =
    s"SELECT shield_id FROM ${OneTestTable.quoted} $keyword SELECT item_id FROM ${TwoTestTable.quoted}"

  "UNION ALL" should "render" in {
    sql(left.unionAll(right)) should matchSQL(combined("UNION ALL"))
  }

  "UNION DISTINCT" should "render" in {
    sql(left.unionDistinct(right)) should matchSQL(combined("UNION DISTINCT"))
  }

  "INTERSECT" should "render, with and without DISTINCT" in {
    sql(left.intersect(right)) should matchSQL(combined("INTERSECT"))
    sql(left.intersectDistinct(right)) should matchSQL(combined("INTERSECT DISTINCT"))
  }

  "EXCEPT" should "render, with and without DISTINCT" in {
    sql(left.except(right)) should matchSQL(combined("EXCEPT"))
    sql(left.exceptDistinct(right)) should matchSQL(combined("EXCEPT DISTINCT"))
  }

  it should "chain with UNION, which shares its precedence" in {
    val third = select(itemId) from ThreeTestTable
    sql(left.except(right).unionAll(third)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} EXCEPT SELECT item_id FROM ${TwoTestTable.quoted} " +
        s"UNION ALL SELECT item_id FROM ${ThreeTestTable.quoted}"
    )
  }

  // `a UNION ALL b INTERSECT c` is `a UNION ALL (b INTERSECT c)`, so a flat Seq in that order would render SQL that
  // does not mean what the call order says.
  "A chain mixing INTERSECT with UNION or EXCEPT" should "be refused rather than reordered" in {
    val third = select(itemId) from ThreeTestTable
    an[IllegalArgumentException] should be thrownBy left.unionAll(right).intersect(third)
    an[IllegalArgumentException] should be thrownBy left.intersect(right).unionAll(third)
    an[IllegalArgumentException] should be thrownBy left.except(right).intersect(third)
  }

  it should "be expressible by nesting the INTERSECT in a subquery" in {
    val third  = select(itemId) from ThreeTestTable
    val nested = select(itemId) from right.intersect(third)
    sql(left.unionAll(nested)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} UNION ALL SELECT item_id FROM " +
        s"(SELECT item_id FROM ${TwoTestTable.quoted} INTERSECT SELECT item_id FROM ${ThreeTestTable.quoted})"
    )
  }

  it should "still allow a chain of INTERSECTs, which share one level" in {
    val third = select(itemId) from ThreeTestTable
    sql(left.intersect(right).intersectDistinct(third)) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} INTERSECT SELECT item_id FROM ${TwoTestTable.quoted} " +
        s"INTERSECT DISTINCT SELECT item_id FROM ${ThreeTestTable.quoted}"
    )
  }

  "Every set operation" should "require matching column counts" in {
    val wider = select(shieldId, itemId) from OneTestTable
    an[IllegalArgumentException] should be thrownBy left.unionDistinct(wider)
    an[IllegalArgumentException] should be thrownBy left.intersect(wider)
    an[IllegalArgumentException] should be thrownBy left.except(wider)
  }
}
