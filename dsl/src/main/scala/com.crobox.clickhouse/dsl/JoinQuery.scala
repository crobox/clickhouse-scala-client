package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

object JoinQuery {

  sealed trait JoinType

  case object AnyInnerJoin extends JoinType

  case object AnyLeftJoin extends JoinType

  case object AnyRightJoin extends JoinType

  case object AllLeftJoin extends JoinType

  case object AllRightJoin extends JoinType

  case object AllInnerJoin extends JoinType

}

case class JoinQuery(`type`: JoinType, other: FromQuery, usingColumns: Set[AnyTableColumn] = Set[AnyTableColumn](), global: Boolean = false)
