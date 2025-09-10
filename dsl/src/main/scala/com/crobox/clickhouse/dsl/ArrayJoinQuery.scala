package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.ArrayJoinQuery._

/**
 * See https://clickhouse.com/docs/sql-reference/statements/select/array-join
 */
object ArrayJoinQuery {

  sealed trait ArrayJoinType

  case object ArrayJoin     extends ArrayJoinType
  case object LeftArrayJoin extends ArrayJoinType
}

/**
 * @param joinType
 *   Type of array join (ARRAY JOIN or LEFT ARRAY JOIN)
 * @param columns
 *   Array columns to join
 */
case class ArrayJoinQuery(
    joinType: ArrayJoinType,
    columns: Seq[Column]
)