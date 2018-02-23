package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats.StringQueryValue
import com.crobox.clickhouse.dsl.{ClickhouseStatement, Column, NativeColumn}
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

  private[Engine] abstract class MergeTreeEngine(val name: String) extends Engine {
    val dateColumn: NativeColumn[LocalDate]
    val primaryKey: Seq[Column]
    val indexGranularity: Int
    val samplingExpression: Option[String]

    private val primaryKeys = primaryKey.map(_.name) ++ samplingExpression

    val arguments = Seq(Some(dateColumn.name), samplingExpression,
      Some(s"(${primaryKeys.mkString(", ")})"), Some(indexGranularity)).flatten

    override def toString: String = s"$name(${arguments.mkString(", ")})"
  }

  case class MergeTree(dateColumn: NativeColumn[LocalDate],
                       primaryKey: Seq[Column],
                       samplingExpression: Option[String] = None,
                       indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
      extends MergeTreeEngine("MergeTree")

  case class ReplacingMergeTree(dateColumn: NativeColumn[LocalDate],
                                primaryKey: Seq[Column],
                                samplingExpression: Option[String] = None,
                                indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
      extends MergeTreeEngine("ReplacingMergeTree")

  case class Replicated(zookeeperPath: String,
                        replicaName: String,
                        engine: MergeTreeEngine) extends Engine {
    override def toString: String = {
      val arguments = Seq(zookeeperPath, replicaName).map(StringQueryValue(_)) ++ engine.arguments
      s"Replicated${engine.name}(${arguments.mkString(", ")})"
    }
  }

}
