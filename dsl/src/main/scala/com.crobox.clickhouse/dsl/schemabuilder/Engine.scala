package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats.StringQueryValue
import com.crobox.clickhouse.dsl.{Column, NativeColumn}
import org.joda.time.LocalDate

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
sealed trait Engine {}

object Engine {
  case object TinyLog extends Engine {

    override def toString: String = "TinyLog"
  }

  case object Log extends Engine {

    override def toString: String = "Log"
  }

  case object Memory extends Engine {

    override def toString: String = "Memory"
  }

  /**
   * https://clickhouse.yandex/docs/en/operations/table_engines/distributed/
   * */
  case class DistributedEngine(cluster: String, database: String, targetTable: String, shardingKey: Option[String])
      extends Engine {
    override def toString: String =
      s"Distributed($cluster, $database, $targetTable${shardingKey.map(key => s" ,$key").getOrElse("")})"
  }

  object MergeTreeEngine {
    val DefaultIndexGranularity = 8192

  }

  private def monthPartitionCompat(dateColumn: NativeColumn[LocalDate]): Seq[String] =
    Seq(s"toYYYYMM(${dateColumn.quoted})")

  sealed abstract class MergeTreeEngine(val name: String) extends Engine {
    val partition: Seq[String]
    val primaryKey: Seq[Column]
    val indexGranularity: Int
    val samplingExpression: Option[String]
    val ttl: Iterable[TTL]

    private val partitionArgument: Option[String] = Option(partition.mkString(", ")).filter(_.nonEmpty)
    private val orderByArgument: Option[String] = Option(
      (primaryKey.map(col => Option(col.quoted)) ++ Seq(samplingExpression)).flatten.mkString(", ")
    ).filter(_.nonEmpty).orElse(Option("tuple()"))
    private val settingsArgument: String = s"index_granularity=$indexGranularity"

    val statements: Seq[String] = Seq(
      partitionArgument.map(partitionExp => s"PARTITION BY ($partitionExp)"),
      orderByArgument.map(cols => s"ORDER BY ($cols)"),
      samplingExpression.map(exp => s"SAMPLE BY $exp"),
      TTL.ttl(ttl),
      Option(s"SETTINGS $settingsArgument")
    ).flatten

    override def toString: String =
      s"""$name
      |${statements.mkString("\n")}""".stripMargin
  }

  case class MergeTree(partition: Seq[String],
                       primaryKey: Seq[Column],
                       samplingExpression: Option[String] = None,
                       indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity,
                       ttl: Iterable[TTL] = Iterable.empty)
      extends MergeTreeEngine("MergeTree")

  object MergeTree {

    def apply(dateColumn: NativeColumn[LocalDate], primaryKey: Seq[Column]): MergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              samplingExpression: Option[String]): MergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression = samplingExpression)

    def apply(dateColumn: NativeColumn[LocalDate], primaryKey: Seq[Column], indexGranularity: Int): MergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, indexGranularity = indexGranularity)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              samplingExpression: Option[String],
              indexGranularity: Int): MergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression, indexGranularity)
  }

  case class ReplacingMergeTree(partition: Seq[String],
                                primaryKey: Seq[Column],
                                samplingExpression: Option[String] = None,
                                indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity,
                                version: Option[Column] = None,
                                ttl: Iterable[TTL] = Iterable.empty)
      extends MergeTreeEngine("ReplacingMergeTree" + version.map(col => s"(${col.name})").getOrElse(""))

  object ReplacingMergeTree {

    def apply(dateColumn: NativeColumn[LocalDate], primaryKey: Seq[Column]): ReplacingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              samplingExpression: Option[String]): ReplacingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression = samplingExpression)

    def apply(dateColumn: NativeColumn[LocalDate], primaryKey: Seq[Column], indexGranularity: Int): ReplacingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, indexGranularity = indexGranularity)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              samplingExpression: Option[String],
              indexGranularity: Int): ReplacingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression, indexGranularity, version = None)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              samplingExpression: Option[String],
              indexGranularity: Int,
              version: Option[Column]): ReplacingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression, indexGranularity, version)
  }

  case class SummingMergeTree(partition: Seq[String],
                              primaryKey: Seq[Column],
                              summingColumns: Seq[Column] = Seq.empty,
                              samplingExpression: Option[String] = None,
                              indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity,
                              ttl: Iterable[TTL] = Iterable.empty)
      extends MergeTreeEngine("SummingMergeTree") {

    override def toString: String = {
      val summingColArg =
        if (summingColumns.isEmpty) ""
        else "((" + summingColumns.map(_.quoted).mkString(", ") + "))"

      s"""$name$summingColArg
         |${statements.mkString("\n")}""".stripMargin
    }
  }

  object SummingMergeTree {

    def apply(dateColumn: NativeColumn[LocalDate], primaryKey: Seq[Column]): SummingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              summingColumns: Seq[Column]): SummingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, summingColumns)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              summingColumns: Seq[Column],
              samplingExpression: Option[String],
              indexGranularity: Int): SummingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, summingColumns, samplingExpression, indexGranularity)
  }

  case class AggregatingMergeTree(partition: Seq[String],
                                  primaryKey: Seq[Column],
                                  samplingExpression: Option[String] = None,
                                  indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity,
                                  ttl: Iterable[TTL] = Iterable.empty)
      extends MergeTreeEngine("AggregatingMergeTree")

  object AggregatingMergeTree {

    def apply(dateColumn: NativeColumn[LocalDate], primaryKey: Seq[Column]): AggregatingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              samplingExpression: Option[String]): AggregatingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression = samplingExpression)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              indexGranularity: Int): AggregatingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, indexGranularity = indexGranularity)

    def apply(dateColumn: NativeColumn[LocalDate],
              primaryKey: Seq[Column],
              samplingExpression: Option[String],
              indexGranularity: Int): AggregatingMergeTree =
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression, indexGranularity)
  }

  case class Replicated(zookeeperPath: String, replicaName: String, engine: MergeTreeEngine) extends Engine {
    override def toString: String = {
      val summingColArg = Seq(engine).collect {
        case s: SummingMergeTree if s.summingColumns.nonEmpty =>
          "(" + s.summingColumns.map(_.quoted).mkString(", ") + ")"
      }

      val replicationArgs = Seq(zookeeperPath, replicaName).map(StringQueryValue(_)) ++ summingColArg

      engine match {
        case x: ReplacingMergeTree if x.version.isDefined =>
          s"""ReplicatedReplacingMergeTree(${(replicationArgs :+ x.version.get.name).mkString(", ")})
             |${engine.statements.mkString("\n")}""".stripMargin
        case _ =>
          s"""Replicated${engine.name}(${replicationArgs.mkString(", ")})
             |${engine.statements.mkString("\n")}""".stripMargin
      }
    }
  }
}
