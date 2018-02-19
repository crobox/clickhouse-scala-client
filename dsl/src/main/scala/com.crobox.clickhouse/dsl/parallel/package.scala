package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery.AllLeftJoin
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

package object parallel {

  implicit class ParellelizableQuery(operationalQuery: OperationalQuery) {

    /**
     * Merging 2 queries will retaining all grouping and selection of both queries and join them using the grouped columns
     */
    def merge(query: OperationalQuery): MergingQueries =
      MergingQueries(operationalQuery, query)

    def combine(query: OperationalQuery): OperationalQuery =
      CumulativeQueries(operationalQuery, query)
  }

  /**
   * The Parallel one just adds the results together...so for 1000 and 1000 you will get a result containing 2000 entries
   */
  case class CumulativeQueries(first: OperationalQuery, second: OperationalQuery) extends OperationalQuery {
    override val internalQuery = null

    override def groupBy(columns: AnyTableColumn*): OperationalQuery = {
      val firstQueryGrouped  = first.groupBy(columns: _*)
      val secondQueryGrouped = second.groupBy(columns: _*)
      this.copy(firstQueryGrouped, secondQueryGrouped)
    }

    override def orderBy(columns: AnyTableColumn*): OperationalQuery = {
      val firstQueryGrouped  = first.orderBy(columns: _*)
      val secondQueryGrouped = second.orderBy(columns: _*)
      this.copy(firstQueryGrouped, secondQueryGrouped)
    }
  }

  /**
   * Smart joins with automated grouping, sorting and then joining on the matching columns
   *
   * @param rightTableQry
   * @param leftTableQry
   */
  case class MergingQueries(rightTableQry: OperationalQuery,
                            leftTableQry: OperationalQuery,
                            joinType: JoinQuery.JoinType = AllLeftJoin)
      extends OperationalQuery {
    override val internalQuery = null

    def on(columns: AnyTableColumn*): OperationalQuery = {
      val rightTableQryGrouped = rightTableQry.groupBy(columns: _*).orderBy(columns: _*)
      val leftTableQryGrouped  = leftTableQry.groupBy(columns: _*).orderBy(columns: _*)

      this._on(rightTableQryGrouped, leftTableQryGrouped, columns)
    }

    def onUngrouped(columns: AnyTableColumn*): OperationalQuery =
      this._on(rightTableQry, leftTableQry, columns)

    def joinWith(joinType: JoinQuery.JoinType): MergingQueries =
      this.copy(joinType = joinType)

    private def _on(rightTableQry: OperationalQuery,
                    leftTableQry: OperationalQuery,
                    groupCols: Seq[AnyTableColumn]): OperationalQuery = {

      def recursiveCollectCols(qry: InternalQuery, cols: Set[AnyTableColumn] = Set.empty): Set[AnyTableColumn] = {
        val uQry = qry

        val selectAll = uQry.select.toSeq.flatMap(_.columns).contains(all())

        val maybeFromCols = uQry.from match {
          case Some(value: InnerFromQuery) if selectAll =>
            recursiveCollectCols(value.innerQuery.internalQuery)
          case _ =>
            Set.empty
        }

        val newCols = cols ++ maybeFromCols ++ uQry.select.toSeq.flatMap(_.columns)

        uQry.join match {
          case Some(JoinQuery(_, q, _)) if selectAll => recursiveCollectCols(q.internalQuery, newCols)
          case _                                               => newCols
        }
      }

      //Forcefully add the columns of the right table(s), because 'select *' on a join only returns the values of the left table in clickhouse
      val joinCols = recursiveCollectCols(rightTableQry.internalQuery)
      //filter out cols that are already available trough grouping
        .filterNot(thisCol => groupCols.exists(_.name == thisCol.name))
        //Map to a simple column so that we just add the select to top level
        .map(origCol => RefColumn(origCol.name))
        .toList
        .filterNot(col => col.name.isEmpty)
        .:+(all())

      select(joinCols: _*)
        .from(leftTableQry)
        .join(joinType, rightTableQry)
        .using(groupCols.head, groupCols.tail: _*)
    }

  }
}
