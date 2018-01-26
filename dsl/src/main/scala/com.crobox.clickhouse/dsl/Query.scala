package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

import scala.collection.mutable

trait Table {
  val name: String
}

trait Query {
  val underlying: UnderlyingQuery
}

case class Limit(size: Long = 100, offset: Long = 0)

trait OrderingDirection

case object ASC extends OrderingDirection

case object DESC extends OrderingDirection

//trait InnerQuery[T <: VirtualTable] extends Query[T]
sealed case class UnderlyingQuery(selectQuery: SelectQuery,
                                  from: FromQuery,
                                  where: Option[Comparison] = None,
                                  groupBy: mutable.LinkedHashSet[AnyTableColumn] = mutable.LinkedHashSet.empty,
                                  join: Option[JoinQuery] = None,
                                  orderBy: mutable.LinkedHashSet[(AnyTableColumn, OrderingDirection)] =
                                    mutable.LinkedHashSet.empty,
                                  limit: Option[Limit] = None)
