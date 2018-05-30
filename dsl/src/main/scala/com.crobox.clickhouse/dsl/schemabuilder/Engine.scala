package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.Column
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats.StringQueryValue

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

  private[Engine] abstract class MergeTreeEngine(val name: String) extends Engine {
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

  case class ReplacingMergeTree(partition: Seq[String],
                                primaryKey: Seq[Column],
                                samplingExpression: Option[String] = None,
                                indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
      extends MergeTreeEngine("ReplacingMergeTree")

  case class Replicated(zookeeperPath: String, replicaName: String, engine: MergeTreeEngine) extends Engine {
    override def toString: String = {
      val replicationArgs = Seq(zookeeperPath, replicaName).map(StringQueryValue(_)).mkString(", ")

      s"""Replicated${engine.name}($replicationArgs)
         |${engine.statements.mkString("\n")}""".stripMargin
    }
  }

}
