package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery._
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

trait JoinableQuery extends Query {

  def allInnerJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(underlying, AllInnerJoin, query)

  def allLeftJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(underlying, AllLeftJoin, query)

  def anyLeftJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(underlying, AnyLeftJoin, query)

  def anyInnerJoin(query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(underlying, AnyInnerJoin, query)

  def join[TargetTable <: Table](`type`: JoinType, query: OperationalQuery): JoinQuery =
    InnerJoinedQuery(underlying, `type`, query)

  def join[TargetTable <: Table](`type`: JoinType, table: TargetTable): JoinQuery =
    TableJoinedQuery(underlying, `type`, table)

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

case class TableJoinedQuery[TargetTable <: Table, OriginalTable <: Table](underlineQuery: UnderlyingQuery,
                                                                          `type`: JoinType,
                                                                          table: Table,
                                                                          usingColumns: Set[AnyTableColumn] =
                                                                            Set[AnyTableColumn]())
    extends JoinQuery {
  override val underlying = underlineQuery.copy(join = Some(this))

  override def using(
      column: AnyTableColumn,
      columns: AnyTableColumn*
  ): OperationalQueryWrapper = {
    val join = TableJoinedQuery(underlineQuery, `type`, table, (columns :+ column).toSet)
      .asInstanceOf[TableJoinedQuery[TargetTable, OriginalTable]]
    OperationalQueryWrapper(underlying.copy(join = Some(join)))
  }
}

case class InnerJoinedQuery(underlineQuery: UnderlyingQuery,
                            `type`: JoinType,
                            joinQuery: Query,
                            usingColumns: Set[AnyTableColumn] = Set[AnyTableColumn]())
    extends JoinQuery {
  override val underlying = underlineQuery.copy(join = Some(this))

  override def using(
      column: AnyTableColumn,
      columns: AnyTableColumn*
  ): OperationalQueryWrapper = {
    val join = InnerJoinedQuery(underlineQuery, `type`, joinQuery, (columns :+ column).toSet)
      .asInstanceOf[InnerJoinedQuery]
    OperationalQueryWrapper(underlying.copy(join = Some(join)))
  }
}
