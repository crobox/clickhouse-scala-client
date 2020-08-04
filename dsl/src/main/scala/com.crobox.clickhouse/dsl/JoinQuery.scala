package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.misc.RandomStringGenerator

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
  case object AllInnerJoin  extends JoinType
  case object AllLeftJoin   extends JoinType
  case object AllRightJoin  extends JoinType
  case object AntiLeftJoin  extends JoinType
  case object AntiRightJoin extends JoinType

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
  case object AnyRightJoin  extends JoinType
  case object AsOfJoin      extends JoinType
  case object AsOfLeftJoin  extends JoinType
  case object SemiLeftJoin  extends JoinType
  case object SemiRightJoin extends JoinType
}

case class JoinQuery(`type`: JoinType,
                     other: FromQuery,
                     joinKeys: Seq[Column] = Seq.empty[Column],
                     global: Boolean = false,
                     alias: Option[String] = None) {
  private[dsl] def getAlias = alias.getOrElse(RandomStringGenerator.random())
}
