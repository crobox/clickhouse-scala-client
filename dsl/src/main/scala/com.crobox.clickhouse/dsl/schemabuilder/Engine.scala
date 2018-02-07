package com.crobox.clickhouse.dsl.schemabuilder

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

  object MergeTreeEngine {
    val DefaultIndexGranularity = 8192

  }

  private[Engine] abstract class MergeTreeEngine(val name: String) extends Engine {
    val dateColumn: NativeColumn[LocalDate]
    val primaryKey: Seq[Column]
    val indexGranularity: Int
    val samplingExpression: Option[String]

    private val samplingString = samplingExpression.map(_ + ", ").getOrElse("")

    override def toString: String =
      s"$name(${dateColumn.name}, $samplingString(${primaryKey.map(_.name).mkString(", ")}), $indexGranularity)"
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

}
