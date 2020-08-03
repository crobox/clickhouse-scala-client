package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery._

/**
  * See https://clickhouse.tech/docs/en/sql-reference/statements/select/join/
  */
object JoinQuery {

  sealed trait JoinType

  // Standard SQL JOIN
  case object InnerJoin      extends JoinType
  case object LeftOuterJoin  extends JoinType
  case object RightOuterJoin extends JoinType
  case object FullOuterJoin  extends JoinType
  case object CrossJoin      extends JoinType

  // CUSTOM CLICKHOUSE JOIN
  case object AsOfJoin      extends JoinType
  case object InnerAnyJoin  extends JoinType
  case object LeftAntiJoin  extends JoinType
  case object LeftAnyJoin   extends JoinType
  case object LeftAsOfJoin  extends JoinType
  case object LeftSemiJoin  extends JoinType
  case object RightAntiJoin extends JoinType
  case object RightAnyJoin  extends JoinType
  case object RightSemiJoin extends JoinType

  // DEPRECATED
  @deprecated(
    "Please use AllInnerJoin. Old ANY INNER|RIGHT|FULL JOINs are disabled by default. Their logic would be " +
    "changed. Old logic is many-to-one for all kinds of ANY JOINs. It's equal to apply distinct for right table keys. " +
    "Default behaviour is reserved for many-to-one LEFT JOIN, one-to-many RIGHT JOIN and one-to-one INNER JOIN. It would " +
    "be equal to apply distinct for keys to right, left and both tables respectively",
    "Clickhouse v20"
  )
  case object AnyInnerJoin extends JoinType
  case object AnyLeftJoin  extends JoinType

  @deprecated(
    "Please use AllRightJoin. Old ANY INNER|RIGHT|FULL JOINs are disabled by default. Their logic would be " +
    "changed. Old logic is many-to-one for all kinds of ANY JOINs. It's equal to apply distinct for right table keys. " +
    "Default behaviour is reserved for many-to-one LEFT JOIN, one-to-many RIGHT JOIN and one-to-one INNER JOIN. It would " +
    "be equal to apply distinct for keys to right, left and both tables respectively",
    "Clickhouse v20"
  )
  case object AnyRightJoin extends JoinType
  case object AllLeftJoin  extends JoinType
  case object AllRightJoin extends JoinType
  case object AllInnerJoin extends JoinType
}

case class JoinQuery(`type`: JoinType,
                     other: FromQuery,
                     usingColumns: Seq[Column] = Seq.empty[Column],
                     global: Boolean = false)
