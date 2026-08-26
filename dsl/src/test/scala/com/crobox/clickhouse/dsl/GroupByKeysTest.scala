package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

import java.time.ZonedDateTime

/**
 * `GROUP BY` used to name its keys rather than render them. `Column.name` for an expression is its argument's name, so
 * `toStartOfDay(ts)` grouped by the raw `ts` -- succeeding, over the wrong buckets -- and an arithmetic expression,
 * built on `EmptyColumn`, became the literal `NULL`.
 */
class GroupByKeysTest extends DslTestSpec {

  private val stamp = NativeColumn[ZonedDateTime]("ts_dt", ColumnType.DateTime)

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  it should "render a function expression as itself" in {
    sql(select(count()).from(OneTestTable).groupBy(toStartOfDay(stamp))) should matchSQL(
      s"SELECT count(), toStartOfDay(ts_dt) FROM ${OneTestTable.quoted} GROUP BY toStartOfDay(ts_dt)"
    )
  }

  it should "render an arithmetic expression as itself" in {
    sql(select(count()).from(OneTestTable).groupBy(intDiv(timestampColumn, 1000))) should matchSQL(
      s"SELECT count(), intDiv(ts, 1000) FROM ${OneTestTable.quoted} GROUP BY intDiv(ts, 1000)"
    )
  }

  it should "render a nested expression as itself" in {
    // shieldId, not the timestamp: toUInt64OrNull rejects numeric input, and an expectation the server would reject
    // is the kind this harness exists to avoid.
    sql(select(count()).from(OneTestTable).groupBy(toDateTime(intDiv(toUInt64rNull(shieldId), 1000)))) should
    matchSQL(
      s"SELECT count(), toDateTime(intDiv(toUInt64OrNull(shield_id), 1000)) FROM ${OneTestTable.quoted} " +
        "GROUP BY toDateTime(intDiv(toUInt64OrNull(shield_id), 1000))"
    )
  }

  it should "still refer to an alias by name" in {
    sql(
      select(toStartOfDay(stamp) as "d", count()).from(OneTestTable).groupBy(ref[ZonedDateTime]("d"))
    ) should matchSQL(
      s"SELECT toStartOfDay(ts_dt) AS d, count() FROM ${OneTestTable.quoted} GROUP BY d"
    )
  }

  it should "still refer to an aliased column by its alias" in {
    val day = toStartOfDay(stamp) as "d"
    sql(select(day, count()).from(OneTestTable).groupBy(day)) should matchSQL(
      s"SELECT toStartOfDay(ts_dt) AS d, count() FROM ${OneTestTable.quoted} GROUP BY d"
    )
  }

  it should "still name a stored column" in {
    sql(select(col2, count()).from(TwoTestTable).groupBy(col2)) should matchSQL(
      s"SELECT column_2, count() FROM ${TwoTestTable.quoted} GROUP BY column_2"
    )
  }

  it should "render a constant rather than NULL" in {
    sql(select(count()).from(OneTestTable).groupBy(const(1))) should matchSQL(
      s"SELECT count(), 1 FROM ${OneTestTable.quoted} GROUP BY 1"
    )
  }

  it should "keep WITH ROLLUP, WITH CUBE and WITH TOTALS attached" in {
    sql(select(count()).from(OneTestTable).groupBy(toStartOfDay(stamp)).withRollup) should include(
      "GROUP BY toStartOfDay(ts_dt) WITH ROLLUP"
    )
    sql(select(count()).from(OneTestTable).groupBy(toStartOfDay(stamp)).withCube) should include(
      "GROUP BY toStartOfDay(ts_dt) WITH CUBE"
    )
    sql(select(count()).from(OneTestTable).groupBy(toStartOfDay(stamp)).withTotals) should include(
      "GROUP BY toStartOfDay(ts_dt) WITH TOTALS"
    )
  }
}
