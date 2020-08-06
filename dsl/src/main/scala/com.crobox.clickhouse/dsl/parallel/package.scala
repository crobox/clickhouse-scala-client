package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.JoinQuery.AllLeftJoin

package object parallel {

  implicit class ParallelizableQuery(operationalQuery: OperationalQuery) {

    /**
     * Merging 2 queries will retaining all grouping and selection of both queries and join them using the grouped columns
     */
    def merge(query: OperationalQuery, alias: Option[String]): MergingQueries =
      MergingQueries(operationalQuery, query, AllLeftJoin, alias)
  }

  /**
   * Smart joins with automated grouping, sorting and then joining on the matching columns
   */
  case class MergingQueries(rightTableQry: OperationalQuery,
                            leftTableQry: OperationalQuery,
                            joinType: JoinQuery.JoinType = AllLeftJoin,
                            alias: Option[String])
      extends QueryFactory {

    override def on(columns: Column*): OperationalQuery = {
      val rightTableQryGrouped = rightTableQry.groupBy(columns: _*).orderBy(columns: _*)
      val leftTableQryGrouped  = leftTableQry.groupBy(columns: _*).orderBy(columns: _*)

      this._on(rightTableQryGrouped, leftTableQryGrouped, columns)
    }

    def onUngrouped(columns: Column*): OperationalQuery =
      this._on(rightTableQry, leftTableQry, columns)

    def joinWith(joinType: JoinQuery.JoinType): MergingQueries =
      this.copy(joinType = joinType)

    private def _on(rightTableQry: OperationalQuery,
                    leftTableQry: OperationalQuery,
                    joinKeys: Seq[Column]): OperationalQuery = {

      def recursiveCollectCols(qry: InternalQuery, cols: Seq[Column] = Seq.empty): Seq[Column] = {
        val uQry = qry

        val selectAll = uQry.select.toSeq.flatMap(_.columns).contains(all())

        val maybeFromCols = uQry.from match {
          case Some(value: InnerFromQuery) if selectAll =>
            recursiveCollectCols(value.innerQuery.internalQuery)
          case Some(value: TableFromQuery[_]) if selectAll =>
            value.table.columns
          case _ =>
            Seq.empty
        }

        val newCols = (cols ++ maybeFromCols ++ uQry.select.toSeq.flatMap(_.columns)).distinct

        uQry.join match {
          case Some(JoinQuery(_, q, _, _, _)) if selectAll => recursiveCollectCols(q.internalQuery, newCols)
          case _                                           => newCols
        }
      }

      //Forcefully add the columns of the right table(s), because 'select *' on a join only returns the values of the left table in clickhouse
      val joinCols = recursiveCollectCols(rightTableQry.internalQuery)
      //filter out cols that are already available trough grouping
        .filterNot(thisCol => joinKeys.exists(_.name == thisCol.name))
        //Map to a simple column so that we just add the select to top level
        .map(origCol => RefColumn(origCol.name))
        .toList
        .filterNot(_.name == EmptyColumn.name)
        .:+(all())
        .distinct

      select(joinCols: _*)
        .from(leftTableQry, None)
        .join(joinType, rightTableQry, alias)
        .using(joinKeys.head, joinKeys.tail: _*)
    }

  }
}
