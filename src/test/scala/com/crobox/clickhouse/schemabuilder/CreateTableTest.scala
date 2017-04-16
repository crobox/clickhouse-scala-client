package com.crobox.clickhouse.schemabuilder

import com.crobox.clickhouse.schemabuilder.DefaultValue.Default
import org.scalatest.{FlatSpecLike, Matchers}

/**
  * @author Sjoerd Mulder
  * @since 30-12-16
  */
class CreateTableTest extends FlatSpecLike with Matchers {

  it should "deny creating invalid tables and columns" in {
    intercept[IllegalArgumentException](
      CreateTable("", Seq(), Engine.TinyLog)
    )
    intercept[IllegalArgumentException](
      CreateTable("abc", Seq(), Engine.TinyLog)
    )
    intercept[IllegalArgumentException](
      CreateTable(".Fool", Seq(Column("a")), Engine.TinyLog)
    )
    intercept[IllegalArgumentException](
      Column(".a")
    )
  }

  it should "make add IF NOT EXISTS" in {
    CreateTable("a", Seq(
      Column("b", ColumnType.String)
    ), Engine.TinyLog, ifNotExists = true, "b").toString should be(
      """CREATE TABLE IF NOT EXISTS b.a (
        |  b String
        |) ENGINE = TinyLog""".stripMargin)

  }

  it should "make a valid CREATE TABLE query" in {
    val result = CreateTable("tiny_log_table", Seq(
      Column("test_column", ColumnType.String),
      Column("test_column2", ColumnType.Int8, Default("expr"))
    ), Engine.TinyLog).toString

    result should be(
      """CREATE TABLE default.tiny_log_table (
        |  test_column String,
        |  test_column2 Int8 DEFAULT expr
        |) ENGINE = TinyLog""".stripMargin)
  }

  it should "make a valid CREATE TABLE query for MergeTree" in {
    val result = CreateTable("merge_tree_table", Seq(
      Column("date", ColumnType.Date),
      Column("client_id", ColumnType.FixedString(16)),
      Column("hit_id", ColumnType.FixedString(16)),
      Column("test_column", ColumnType.String),
      Column("test_column2", ColumnType.Int8, Default("2"))
    ), Engine.MergeTree("date", Seq("date", "client_id", "hit_id"), Some("int64Hash(client_id)"))).toString

    result should be(
      """CREATE TABLE default.merge_tree_table (
        |  date Date,
        |  client_id FixedString(16),
        |  hit_id FixedString(16),
        |  test_column String,
        |  test_column2 Int8 DEFAULT 2
        |) ENGINE = MergeTree(date, int64Hash(client_id), (date, client_id, hit_id), 8192)""".stripMargin)
  }

}
