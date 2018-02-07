package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

trait JoinableQuery extends Query {

  def allInnerJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(internalQuery, AllInnerJoin, query)

  def allLeftJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(internalQuery, AllLeftJoin, query)

  def anyLeftJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(internalQuery, AnyLeftJoin, query)

  def anyInnerJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(internalQuery, AnyInnerJoin, query)

  def join[TargetTable <: Table](`type`: JoinType, query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(internalQuery, `type`, query)

  def join[TargetTable <: Table](`type`: JoinType, table: TargetTable): JoinQuery =
    TableJoinedQuery(internalQuery, `type`, table)

}

trait JoinQuery extends Query {

  def using(
      column: AnyTableColumn,
      columns: AnyTableColumn*
  ): OperationalQueryWrapper
}

object JoinQuery {

  trait JoinType

  case object AnyInnerJoin extends JoinType

  case object AnyLeftJoin extends JoinType

  case object AnyRightJoin extends JoinType

  case object AllLeftJoin extends JoinType

  case object AllRightJoin extends JoinType

  case object AllInnerJoin extends JoinType

}

case class TableJoinedQuery[TargetTable <: Table, OriginalTable <: Table](internal: InternalQuery,
                                                                          `type`: JoinType,
                                                                          table: Table,
                                                                          usingColumns: Set[AnyTableColumn] =
                                                                            Set[AnyTableColumn]())
    extends JoinQuery {
  override val internalQuery = internal.copy(join = Some(this))

  override def using(
      column: AnyTableColumn,
      columns: AnyTableColumn*
  ): OperationalQueryWrapper = {
    val join = TableJoinedQuery(internal, `type`, table, (columns :+ column).toSet)
      .asInstanceOf[TableJoinedQuery[TargetTable, OriginalTable]]
    OperationalQueryWrapper(internalQuery.copy(join = Some(join)))
  }
}

case class InnerJoinedQuery(internal: InternalQuery,
                            `type`: JoinType,
                            joinQuery: Query,
                            usingColumns: Set[AnyTableColumn] = Set[AnyTableColumn]())
    extends JoinQuery {
  override val internalQuery = internal.copy(join = Some(this))

  override def using(
      column: AnyTableColumn,
      columns: AnyTableColumn*
  ): OperationalQueryWrapper = {
    val join = InnerJoinedQuery(internal, `type`, joinQuery, (columns :+ column).toSet)
      .asInstanceOf[InnerJoinedQuery]
    OperationalQueryWrapper(internalQuery.copy(join = Some(join)))
  }
}
