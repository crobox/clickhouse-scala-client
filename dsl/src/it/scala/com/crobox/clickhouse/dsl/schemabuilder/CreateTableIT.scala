package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._
import org.joda.time.DateTime

class CreateTableIT extends DslITSpec {

  //
  // https://clickhouse.com/docs/en/sql-reference/statements/alter/ttl/
  //

  it should "create table with TTL" in {
    val eventTime = NativeColumn[DateTime]("event_time", ColumnType.DateTime)
    val userId    = NativeColumn[Long]("user_id", ColumnType.UInt64)
    val comment   = NativeColumn[String]("comment", ColumnType.String)
    val db        = database
    val create    = CreateTable(
      table = new Table {
        override val database: String              = db
        override val name: String                  = "table_with_ttl"
        override val columns: Seq[NativeColumn[_]] = Seq(eventTime, userId, comment)
      },
      Engine.MergeTree(partition = Seq.empty, primaryKey = Seq(), ttl = Option(TTL(eventTime, "3 MONTH")))
    )

    // create database
    val full = create.table.quoted
    clickClient.execute(s"DROP TABLE IF EXISTS $full").futureValue
    clickClient.execute(create.query).futureValue

    // now add entries
    clickClient.execute(s"INSERT INTO $full VALUES (now(), 1, 'username1')").futureValue
    clickClient.execute(s"INSERT INTO $full VALUES (now() - INTERVAL 4 MONTH, 2, 'username2')").futureValue
    clickClient.execute(s"OPTIMIZE TABLE $full FINAL").futureValue
    clickClient.query(s"SELECT user_id FROM $full").futureValue should matchSQL("1")

    // remove TTL
    clickClient.execute(s"ALTER TABLE $full REMOVE TTL").futureValue

    // Run again (now two records)
    clickClient.execute(s"INSERT INTO $full VALUES (now() - INTERVAL 4 MONTH, 2, 'username2')").futureValue
    clickClient.execute(s"OPTIMIZE TABLE $full FINAL").futureValue
    clickClient.query(s"SELECT user_id FROM $full").futureValue should matchSQL("1 2")
  }
}
