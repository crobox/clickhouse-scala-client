package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.DefaultValue.Default
import com.crobox.clickhouse.dsl.schemabuilder.Engine.{DistributedEngine, SummingMergeTree}
import org.joda.time.LocalDate

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
class CreateTableTest extends DslTestSpec {

  case class TestTable(override val name: String,
                       override val columns: Seq[NativeColumn[_]],
                       override val database: String = "default")
      extends Table

  it should "deny creating invalid tables and columns" in {
    intercept[IllegalArgumentException](
      CreateTable(TestTable("", List.empty[NativeColumn[_]]), Engine.TinyLog)
    )
    intercept[IllegalArgumentException](
      CreateTable(TestTable("abc", List()), Engine.TinyLog)
    )
  }

  it should "quote invalid names" in {
    CreateTable(TestTable(".Fool", List(NativeColumn(".a"))), Engine.TinyLog).toString should be(
      """CREATE TABLE default.`.Fool` (
        |  `.a` String
        |) ENGINE = TinyLog""".stripMargin
    )

  }

  it should "make add IF NOT EXISTS" in {
    CreateTable(TestTable("a",
                          List(
                            NativeColumn("b", ColumnType.String)
                          ),
                          "b"),
                Engine.TinyLog,
                ifNotExists = true,
    ).toString should be("""CREATE TABLE IF NOT EXISTS b.a (
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
    val testColumn3 = NativeColumn("test_column3", ColumnType.LowCardinality(ColumnType.String), Default("<default>"))
    val testColumn4 = NativeColumn("test_column4", ColumnType.Nullable(ColumnType.UUID))
    val result = CreateTable(
      TestTable(
        "merge_tree_table",
        Seq(
          date,
          clientId,
          hitId,
          testColumn,
          testColumn2,
          testColumn3,
          testColumn4
        )
      ),
      Engine.MergeTree(Seq(s"toYYYYMM(${date.name})"), Seq(date, clientId, hitId), Some("int64Hash(client_id)"))
    ).toString

    result should be("""CREATE TABLE default.merge_tree_table (
        |  date Date,
        |  client_id FixedString(16),
        |  hit_id FixedString(16),
        |  test_column String,
        |  test_column2 Int8 DEFAULT 2,
        |  test_column3 LowCardinality(String) DEFAULT <default>,
        |  test_column4 Nullable(UUID)
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
      Engine.MergeTree(Seq(clientId.name, s"toYYYYMM(${date.name})"),
                       Seq(date, clientId, hitId),
                       Some("int64Hash(client_id)"))
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

  it should "support the old create statement syntax with month partition" in {
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
      Engine.MergeTree(date, Seq(date, clientId, hitId), Some("int64Hash(client_id)"))
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

  lazy val replacingMergeTree = {
    val date          = NativeColumn[LocalDate]("date", ColumnType.Date)
    val clientId      = NativeColumn("client_id", ColumnType.FixedString(16))
    val hitId         = NativeColumn("hit_id", ColumnType.FixedString(16))
    val testColumn    = NativeColumn("test_column", ColumnType.String)
    val testColumn2   = NativeColumn("test_column2", ColumnType.Int8, Default("2"))
    val versionColumn = NativeColumn("version", ColumnType.UInt8)
    CreateTable(
      TestTable(
        "merge_tree_table",
        Seq(
          date,
          clientId,
          hitId,
          testColumn,
          testColumn2,
          versionColumn
        )
      ),
      Engine.ReplacingMergeTree(Seq(s"toYYYYMM(${date.name})"),
                                Seq(date, clientId, hitId),
                                Some("int64Hash(client_id)"),
                                version = Option(versionColumn))
    )
  }

  it should "make a valid CREATE TABLE query for ReplacingMergeTree" in {
    val result = replacingMergeTree.toString
    result should be("""CREATE TABLE default.merge_tree_table (
        |  date Date,
        |  client_id FixedString(16),
        |  hit_id FixedString(16),
        |  test_column String,
        |  test_column2 Int8 DEFAULT 2,
        |  version UInt8
        |) ENGINE = ReplacingMergeTree(version)
        |PARTITION BY (toYYYYMM(date))
        |ORDER BY (date, client_id, hit_id, int64Hash(client_id))
        |SAMPLE BY int64Hash(client_id)
        |SETTINGS index_granularity=8192""".stripMargin)
  }

  it should "make a valid CREATE TABLE query for ReplicatedReplacingMergeTree" in {
    val result = replacingMergeTree
      .copy(
        engine = Engine.Replicated(
          "/zookeeper/{item}",
          "{replica}",
          replacingMergeTree.engine
            .asInstanceOf[Engine.ReplacingMergeTree]
            .copy(samplingExpression = None)
        )
      )
      .toString
    result should be("""CREATE TABLE default.merge_tree_table (
                       |  date Date,
                       |  client_id FixedString(16),
                       |  hit_id FixedString(16),
                       |  test_column String,
                       |  test_column2 Int8 DEFAULT 2,
                       |  version UInt8
                       |) ENGINE = ReplicatedReplacingMergeTree('/zookeeper/{item}', '{replica}', version)
                       |PARTITION BY (toYYYYMM(date))
                       |ORDER BY (date, client_id, hit_id)
                       |SETTINGS index_granularity=8192""".stripMargin)
  }

  it should "create a table with an AggregatingMergeTree engine" in {
    val date     = NativeColumn[LocalDate]("date", ColumnType.Date)
    val clientId = NativeColumn("client_id", ColumnType.FixedString(16))
    val uniqHits =
      NativeColumn[StateResult[Long]]("hits", ColumnType.AggregateFunctionColumn("uniq", ColumnType.String))

    val create = CreateTable(
      TestTable(
        "test_table_agg",
        Seq(date, clientId, uniqHits)
      ),
      Engine.AggregatingMergeTree(Seq(s"toYYYYMM(${date.name})"), Seq(date, clientId))
    )

    create.toString should be("""CREATE TABLE default.test_table_agg (
        |  date Date,
        |  client_id FixedString(16),
        |  hits AggregateFunction(uniq, String)
        |) ENGINE = AggregatingMergeTree
        |PARTITION BY (toYYYYMM(date))
        |ORDER BY (date, client_id)
        |SETTINGS index_granularity=8192""".stripMargin)
  }

  it should "create a table with an SummingMergeTree engine" in {
    val date           = NativeColumn[LocalDate]("date", ColumnType.Date)
    val client_count   = NativeColumn("client_count", ColumnType.UInt8)
    val summingColumns = Seq(client_count)

    val create = CreateTable(
      TestTable(
        "test_table_agg",
        Seq(date, client_count)
      ),
      SummingMergeTree(date, Seq(date), summingColumns)
    )

    create.toString should be("""CREATE TABLE default.test_table_agg (
        |  date Date,
        |  client_count UInt8
        |) ENGINE = SummingMergeTree((client_count))
        |PARTITION BY (toYYYYMM(date))
        |ORDER BY (date)
        |SETTINGS index_granularity=8192""".stripMargin)
  }

  "Distributed" should "create table with distributed engine" in {
    val date = NativeColumn[LocalDate]("date", ColumnType.Date)
    val create = CreateTable(
      TestTable("distributed_table", Seq(date)),
      DistributedEngine("target_table_cluster", "target_database", "target_table", Some("sipHash(gig)")),
      clusterName = Some("cluster")
    )
    create.toString should be(
      """CREATE TABLE default.distributed_table ON CLUSTER cluster (
        |  date Date
        |) ENGINE = Distributed(target_table_cluster, target_database, target_table ,sipHash(gig))""".stripMargin
    )
  }

  it should "create a table with an AggregatingMergeTree engine with TTL" in {
    val date     = NativeColumn[LocalDate]("date", ColumnType.Date)
    val clientId = NativeColumn("client_id", ColumnType.FixedString(16))
    val uniqHits =
      NativeColumn[StateResult[Long]]("hits", ColumnType.AggregateFunctionColumn("uniq", ColumnType.String))

    val create = CreateTable(
      TestTable(
        "test_table_agg",
        Seq(date, clientId, uniqHits)
      ),
      Engine.AggregatingMergeTree(Seq(s"toYYYYMM(${date.name})"),
                                  Seq(date, clientId),
                                  ttl = Option(TTLEntry(date, "3 MONTH")))
    )

    create.toString should be("""CREATE TABLE default.test_table_agg (
        |  date Date,
        |  client_id FixedString(16),
        |  hits AggregateFunction(uniq, String)
        |) ENGINE = AggregatingMergeTree
        |PARTITION BY (toYYYYMM(date))
        |ORDER BY (date, client_id)
        |TTL date + INTERVAL 3 MONTH
        |SETTINGS index_granularity=8192""".stripMargin)
  }

  it should "create a table with an AggregatingMergeTree engine with multiple TTL's" in {
    val date     = NativeColumn[LocalDate]("date", ColumnType.Date)
    val clientId = NativeColumn("client_id", ColumnType.FixedString(16))
    val uniqHits =
      NativeColumn[StateResult[Long]]("hits", ColumnType.AggregateFunctionColumn("uniq", ColumnType.String))

    val create = CreateTable(
      TestTable(
        "test_table_agg",
        Seq(date, clientId, uniqHits)
      ),
      Engine.AggregatingMergeTree(
        Seq(s"toYYYYMM(${date.name})"),
        Seq(date, clientId),
        ttl = Iterable(TTLEntry(date, "1 MONTH [DELETE]"),
                       TTLEntry(date, "1 WEEK TO VOLUME 'aaa'"),
                       TTLEntry(date, "2 WEEK TO DISK 'bbb'"))
      )
    )

    create.toString should matchSQL("""CREATE TABLE default.test_table_agg (
        |  date Date,
        |  client_id FixedString(16),
        |  hits AggregateFunction(uniq, String)
        |) ENGINE = AggregatingMergeTree
        |PARTITION BY (toYYYYMM(date))
        |ORDER BY (date, client_id)
        |TTL date + INTERVAL 1 MONTH [DELETE],
        |    date + INTERVAL 1 WEEK TO VOLUME 'aaa',
        |    date + INTERVAL 2 WEEK TO DISK 'bbb'
        |SETTINGS index_granularity=8192""".stripMargin)
  }
}
