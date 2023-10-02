package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.GroupByQuery.GroupByMode

object GroupByQuery {

  sealed trait GroupByMode

  case object WithRollup extends GroupByMode
  case object WithCube extends GroupByMode

}

case class GroupByQuery(usingColumns: Seq[Column] = Seq.empty,
                        mode: Option[GroupByMode] = None,
                        withTotals: Boolean = false)