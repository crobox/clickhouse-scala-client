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

/**
 * @param joinType
 * @param other
 * @param on Expressions. Column Operator Column, where operator must be one of the following: =, >, >=, <, <=.
 *           Default set to '='
 * @param using Columns
 * @param global
 */
case class JoinQuery(joinType: JoinType,
                     other: FromQuery,
                     on: Seq[JoinCondition] = Seq.empty,
                     using: Seq[Column] = Seq.empty,
                     global: Boolean = false)

case class JoinCondition(left: Column, operator: String, right: Column) {

  assert(JoinCondition.SupportedOperators.contains(operator),
         s"Operator[$operator] must be one of: ${JoinCondition.SupportedOperators}")
}

object JoinCondition {
  val SupportedOperators = Set(">", ">=", "<", "<=", "=")

  def apply(column: Column): JoinCondition                   = JoinCondition(column, "=", column)
  def apply(triple: (Column, String, Column)): JoinCondition = JoinCondition(triple._1, triple._2, triple._3)
}
