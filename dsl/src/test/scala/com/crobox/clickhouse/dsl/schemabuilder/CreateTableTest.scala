package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.{NativeColumn, RefColumn, TableColumn, UInt32}
import com.crobox.clickhouse.dsl.TestSchema.TestTable
import com.crobox.clickhouse.dsl.schemabuilder.DefaultValue.Default
import org.joda.time.LocalDate
import org.scalatest.{FlatSpecLike, Matchers}

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
class CreateTableTest extends FlatSpecLike with Matchers {

  it should "deny creating invalid tables and columns" in {
    intercept[IllegalArgumentException](
      CreateTable(TestTable("", List.empty[NativeColumn[_]]), Engine.TinyLog)
    )
    intercept[IllegalArgumentException](
      CreateTable(TestTable("abc", List()), Engine.TinyLog)
    )
    intercept[IllegalArgumentException](
      CreateTable(TestTable(".Fool", List(NativeColumn("a"))), Engine.TinyLog)
    )
    intercept[IllegalArgumentException](
      NativeColumn(".a")
    )
  }

  it should "make add IF NOT EXISTS" in {
    CreateTable(TestTable("a",
                          List(
                            NativeColumn("b", ColumnType.String)
                          )),
                Engine.TinyLog,
                ifNotExists = true,
                "b").toString should be("""CREATE TABLE IF NOT EXISTS b.a (
        |  b String
        |) ENGINE = TinyLog""".stripMargin)

  }

  it should "make add ON CLUSTER" in {
    CreateTable(TestTable("a",
      List(
        NativeColumn("b", ColumnType.String)
      )),
      Engine.TinyLog,
      clusterName = Some("mycluster")).toString should be("""CREATE TABLE default.a ON CLUSTER mycluster (
                                |  b String
                                |) ENGINE = TinyLog""".stripMargin)

  }

  it should "make a valid CREATE TABLE query" in {
    val result = CreateTable(
      TestTable("tiny_log_table",
        Seq(
                  NativeColumn("test_column", ColumnType.String),
                  NativeColumn("test_column2", ColumnType.Int8, Default("expr"))
                )),
      Engine.TinyLog
    ).toString

    result should be("""CREATE TABLE default.tiny_log_table (
        |  test_column String,
        |  test_column2 Int8 DEFAULT expr
        |) ENGINE = TinyLog""".stripMargin)
  }

  it should "make a valid CREATE TABLE query for MergeTree" in {
    val date        = NativeColumn[LocalDate]("date", ColumnType.Date)
    val clientId    = NativeColumn("client_id", ColumnType.FixedString(16))
    val hitId       = NativeColumn("hit_id", ColumnType.FixedString(16))
    val testColumn  = NativeColumn("test_column", ColumnType.String)
    val testColumn2 = NativeColumn("test_column2", ColumnType.Int8, Default("2"))
    val result = CreateTable(
      TestTable(
        "merge_tree_table",
        Seq(
          date,
          clientId,
          hitId,
          testColumn,
          testColumn2
        )
      ),
      Engine.MergeTree(Seq(s"toYYYYMM(${date.name})"), Seq(date, clientId, hitId), Some("int64Hash(client_id)"))
    ).toString

    result should be("""CREATE TABLE default.merge_tree_table (
        |  date Date,
        |  client_id FixedString(16),
        |  hit_id FixedString(16),
        |  test_column String,
        |  test_column2 Int8 DEFAULT 2
        |) ENGINE = MergeTree
        |PARTITION BY (toYYYYMM(date))
        |ORDER BY (date, client_id, hit_id, int64Hash(client_id))
        |SAMPLE BY int64Hash(client_id)
        |SETTINGS index_granularity=8192""".stripMargin)
  }

  it should "make a valid CREATE TABLE query for MergeTree with a custom partition key" in {
    val date        = NativeColumn[LocalDate]("date", ColumnType.Date)
    val clientId    = NativeColumn("client_id", ColumnType.FixedString(16))
    val hitId       = NativeColumn("hit_id", ColumnType.FixedString(16))
    val testColumn  = NativeColumn("test_column", ColumnType.String)
    val testColumn2 = NativeColumn("test_column2", ColumnType.Int8, Default("2"))
    val result = CreateTable(
      TestTable(
        "merge_tree_table",
        Seq(
          date,
          clientId,
          hitId,
          testColumn,
          testColumn2
        )
      ),
      Engine.MergeTree(Seq(clientId.name, s"toYYYYMM(${date.name})"), Seq(date, clientId, hitId), Some("int64Hash(client_id)"))
    ).toString

    result should be("""CREATE TABLE default.merge_tree_table (
                       |  date Date,
                       |  client_id FixedString(16),
                       |  hit_id FixedString(16),
                       |  test_column String,
                       |  test_column2 Int8 DEFAULT 2
                       |) ENGINE = MergeTree
                       |PARTITION BY (client_id, toYYYYMM(date))
                       |ORDER BY (date, client_id, hit_id, int64Hash(client_id))
                       |SAMPLE BY int64Hash(client_id)
                       |SETTINGS index_granularity=8192""".stripMargin)
  }

  lazy val replacingMergeTree = {
    val date        = NativeColumn[LocalDate]("date", ColumnType.Date)
    val clientId    = NativeColumn("client_id", ColumnType.FixedString(16))
    val hitId       = NativeColumn("hit_id", ColumnType.FixedString(16))
    val testColumn  = NativeColumn("test_column", ColumnType.String)
    val testColumn2 = NativeColumn("test_column2", ColumnType.Int8, Default("2"))
    CreateTable(
      TestTable(
        "merge_tree_table",
        Seq(
          date,
          clientId,
          hitId,
          testColumn,
          testColumn2
        )
      ),
      Engine.ReplacingMergeTree(Seq(s"toYYYYMM(${date.name})"), Seq(date, clientId, hitId), Some("int64Hash(client_id)"))
    )
  }

  it should "make a valid CREATE TABLE query for ReplacingMergeTree" in {
    val result = replacingMergeTree.toString
    result should be("""CREATE TABLE default.merge_tree_table (
        |  date Date,
        |  client_id FixedString(16),
        |  hit_id FixedString(16),
        |  test_column String,
        |  test_column2 Int8 DEFAULT 2
        |) ENGINE = ReplacingMergeTree
        |PARTITION BY (toYYYYMM(date))
        |ORDER BY (date, client_id, hit_id, int64Hash(client_id))
        |SAMPLE BY int64Hash(client_id)
        |SETTINGS index_granularity=8192""".stripMargin)
  }

  it should "make a valid CREATE TABLE query for ReplicatedReplacingMergeTree" in {
    val result = replacingMergeTree.copy(engine =
      Engine.Replicated("/zookeeper/{item}", "{replica}",
        replacingMergeTree.engine
          .asInstanceOf[Engine.ReplacingMergeTree]
          .copy(samplingExpression = None)
      )
    ).toString
    result should be("""CREATE TABLE default.merge_tree_table (
                       |  date Date,
                       |  client_id FixedString(16),
                       |  hit_id FixedString(16),
                       |  test_column String,
                       |  test_column2 Int8 DEFAULT 2
                       |) ENGINE = ReplicatedReplacingMergeTree('/zookeeper/{item}', '{replica}')
                       |PARTITION BY (toYYYYMM(date))
                       |ORDER BY (date, client_id, hit_id)
                       |SETTINGS index_granularity=8192""".stripMargin)
  }

}
