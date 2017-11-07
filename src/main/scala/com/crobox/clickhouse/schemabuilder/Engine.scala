package com.crobox.clickhouse.schemabuilder

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
sealed trait Engine

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

  private[Engine] abstract class MergeTreeEngine(engine: String) extends Engine {
    val dateColumn: String
    val primaryKey: Seq[String]
    val indexGranularity: Int
    val samplingExpression: Option[String]

    private val samplingString = samplingExpression.map(_ + ", ").getOrElse("")

    override def toString: String =
      s"$engine($dateColumn, $samplingString(${primaryKey.mkString(", ")}), $indexGranularity)"
  }

  case class MergeTree(dateColumn: String,
                       primaryKey: Seq[String],
                       samplingExpression: Option[String] = None,
                       indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
      extends MergeTreeEngine("MergeTree")

  case class ReplacingMergeTree(dateColumn: String,
                                primaryKey: Seq[String],
                                samplingExpression: Option[String] = None,
                                indexGranularity: Int = MergeTreeEngine.DefaultIndexGranularity)
      extends MergeTreeEngine("ReplacingMergeTree")

}
