package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

import java.time.ZonedDateTime

/**
 * Creates a real table for each type, because the rendering is only right if the server accepts the DDL. Verified
 * against 25.3 that none of these needs an experimental flag.
 */
class ModernColumnTypeIT extends DslITSpec {

  // `suffix`, not `name`: inside `new Table { override val name = ... }` a parameter called `name` is shadowed by
  // the member being defined, which reads as null.
  private def createWith(suffix: String, columnType: ColumnType): Unit =
    createWith(suffix, columnType, columnType.toString)

  private def createWith(suffix: String, columnType: ColumnType, expected: String): Unit = {
    val db     = database
    val column = NativeColumn[String]("c", columnType)
    val create = CreateTable(
      table = new Table {
        override val database: String              = db
        override val name: String                  = s"modern_$suffix"
        override val columns: Seq[NativeColumn[_]] = Seq(column)
      },
      Engine.Memory
    )
    clickClient.execute(s"DROP TABLE IF EXISTS ${create.table.quoted}").futureValue
    clickClient.execute(create.query).futureValue
    // The server's own rendering of what it stored, which is the real confirmation.
    val stored = clickClient
      .query(s"SELECT type FROM system.columns WHERE database = '$db' AND table = 'modern_$suffix' AND name = 'c'")
      .futureValue
      .trim
      // TabSeparated escapes the quotes in a timezone.
      .replace("\\'", "'")
    stored shouldBe expected
  }

  it should "create a DateTime64 column" in createWith("dt64", ColumnType.DateTime64(3))

  it should "create a DateTime64 column with a timezone" in
    createWith("dt64tz", ColumnType.DateTime64(6, Option("Europe/Amsterdam")))

  // ClickHouse normalises a Variant's alternatives into sorted order, so what comes back is not what was declared.
  it should "create a Variant column" in
    createWith("variant", ColumnType.Variant(ColumnType.UInt64, ColumnType.String), "Variant(String, UInt64)")

  it should "create a Dynamic column" in createWith("dynamic", ColumnType.Dynamic)

  it should "create a JSON column" in createWith("json", ColumnType.JSON)

  it should "create a SimpleAggregateFunction column" in
    createWith("simpleagg", ColumnType.SimpleAggregateFunction("sum", ColumnType.UInt64))

  it should "read a DateTime64 value back with its sub-second part" in {
    val db     = database
    val column = NativeColumn[ZonedDateTime]("c", ColumnType.DateTime64(9))
    val create = CreateTable(
      table = new Table {
        override val database: String              = db
        override val name: String                  = "modern_dt64_read"
        override val columns: Seq[NativeColumn[_]] = Seq(column)
      },
      Engine.Memory
    )
    clickClient.execute(s"DROP TABLE IF EXISTS ${create.table.quoted}").futureValue
    clickClient.execute(create.query).futureValue
    clickClient
      .execute(s"INSERT INTO ${create.table.quoted} VALUES ('2026-08-21 22:00:33.250000001')")
      .futureValue

    val row = queryExecutor.executeRows(select(column).from(create.table)).futureValue.rows.head
    row.get(column).map(_.getNano) shouldBe Some(250000001)
  }
}
