package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import org.scalatest.prop.TableDrivenPropertyChecks

class JoinQueryTest extends ClickhouseClientSpec with TableDrivenPropertyChecks with TestSchema {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}
  val database            = "join_query"

  forAll(
    Table(
      ("joinType", "result"),
      (JoinQuery.InnerJoin, "INNER JOIN"),
      (JoinQuery.LeftOuterJoin, "LEFT OUTER JOIN"),
      (JoinQuery.RightOuterJoin, "RIGHT OUTER JOIN"),
      (JoinQuery.FullOuterJoin, "FULL OUTER JOIN"),
      (JoinQuery.CrossJoin, "CROSS JOIN"),
      (JoinQuery.AntiLeftJoin, "ANTI LEFT JOIN"),
      (JoinQuery.AntiRightJoin, "ANTI RIGHT JOIN"),
      (JoinQuery.AnyInnerJoin, "ANY INNER JOIN"),
      (JoinQuery.AnyLeftJoin, "ANY LEFT JOIN"),
      (JoinQuery.AnyRightJoin, "ANY RIGHT JOIN"),
      (JoinQuery.AsOfJoin, "ASOF JOIN"),
      (JoinQuery.AsOfLeftJoin, "ASOF LEFT JOIN"),
      (JoinQuery.SemiLeftJoin, "SEMI LEFT JOIN"),
      (JoinQuery.SemiRightJoin, "SEMI RIGHT JOIN"),
    )
  ) { (joinType, result) =>
    it should s"join correctly on: $joinType" in {
      val query: OperationalQuery =
        select(itemId).from(select(itemId).from(TwoTestTable).join(joinType, ThreeTestTable).using(itemId))
      val sql = clickhouseTokenizer.toSql(query.internalQuery)
      sql should be(
        s"SELECT item_id FROM (SELECT item_id FROM join_query.twoTestTable $result (SELECT * FROM join_query.threeTestTable) USING item_id ) FORMAT JSON"
      )
    }
  }
}
