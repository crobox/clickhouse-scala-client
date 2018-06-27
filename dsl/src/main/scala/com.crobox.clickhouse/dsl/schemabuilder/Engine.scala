package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.{Column, NativeColumn}
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats.StringQueryValue
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

  object MergeTreeEngine {
    val DefaultIndexGranularity = 8192

  }

  private def monthPartitionCompat(dateColumn: NativeColumn[LocalDate]): Seq[String] =
    Seq(s"toYYYYMM(${dateColumn.name})")

  sealed abstract class MergeTreeEngine(val name: String) extends Engine {
    val partition: Seq[String]
    val primaryKey: Seq[Column]
    val indexGranularity: Int
    val samplingExpression: Option[String]

    private val partitionArgument = Option(partition.mkString(", ")).filter(_.nonEmpty)
    private val orderByArgument = Option(
      (primaryKey.map(col => Option(col.name)) ++ Seq(samplingExpression)).flatten.mkString(", ")
    ).filter(_.nonEmpty)
    private val settingsArgument = s"index_granularity=$indexGranularity"

    val statements = Seq(
      partitionArgument.map(partitionExp => s"PARTITION BY ($partitionExp)"),
      orderByArgument.map(cols => s"ORDER BY ($cols)"),
      samplingExpression.map(exp => s"SAMPLE BY $exp"),
      Option(s"SETTINGS $settingsArgument")
    ).flatten

    override def toString: String =
      s"""$name
      |${statements.mkString("\n")}""".stripMargin
  }

  case class MergeTree(partition: Seq[String],
                       primaryKey: Seq[Column],
                       samplingExpression: Option[String] = None,
                       indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
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
                                indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
      extends MergeTreeEngine("ReplacingMergeTree")

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
      apply(monthPartitionCompat(dateColumn), primaryKey, samplingExpression, indexGranularity)
  }

  case class AggregatingMergeTree(partition: Seq[String],
                                  primaryKey: Seq[Column],
                                  samplingExpression: Option[String] = None,
                                  indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
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
      val replicationArgs = Seq(zookeeperPath, replicaName).map(StringQueryValue(_)).mkString(", ")

      s"""Replicated${engine.name}($replicationArgs)
         |${engine.statements.mkString("\n")}""".stripMargin
    }
  }

}
