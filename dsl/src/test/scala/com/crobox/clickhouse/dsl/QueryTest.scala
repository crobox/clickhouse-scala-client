package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.dsl.JoinQuery.AnyInnerJoin
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.testkit.ClickhouseClientSpec

import scala.util.{Failure, Success}

class QueryTest extends ClickhouseClientSpec with TestSchema {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}

  "querying using the typed query" should "perform simple select" in {
    val query = select(shieldId) from OneTestTable
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT shield_id FROM $tokenizerDatabase.captainAmerica FORMAT JSON"
    )
  }

  it should "generate for join between tables" in {
    val query = select(col1, shieldId).from(OneTestTable).join(AnyInnerJoin, TwoTestTable) using shieldId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT column_1, shield_id FROM $tokenizerDatabase.captainAmerica ANY INNER JOIN (SELECT * FROM $tokenizerDatabase.twoTestTable) USING shield_id FORMAT JSON"
    )
  }

  it should "generate inner join" in {
    val expectedUUID                     = UUID.randomUUID()
    val innerQuery: OperationalQuery     = select(shieldId as itemId) from OneTestTable where shieldId.isEq(expectedUUID)
    val joinInnerQuery: OperationalQuery = select(itemId) from TwoTestTable where (col3 isEq "wompalama")
    val query                            = select(col1, shieldId) from innerQuery join (AnyInnerJoin, joinInnerQuery) using itemId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT column_1, shield_id FROM (SELECT shield_id AS item_id FROM $tokenizerDatabase.captainAmerica WHERE shield_id = '$expectedUUID' ) ANY INNER JOIN (SELECT item_id FROM $tokenizerDatabase.twoTestTable WHERE column_3 = 'wompalama' ) USING item_id FORMAT JSON"
    )
  }

  it should "propagate the queryvalue parser from super, when negating isIn()" in {
    val ids = Seq.fill(3)(UUID.randomUUID())
    val query = select(shieldId) from OneTestTable where shieldId.not().isIn(ids.toSet)
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE shield_id NOT IN (${ids.map(s => s"'$s'").mkString(",")}) FORMAT JSON"
    )
  }

  it should "escape from evil" in {
    val query = select(shieldId) from OneTestTable where col3.isEq("use ' evil")
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT shield_id FROM $tokenizerDatabase.captainAmerica WHERE column_3 = 'use \\' evil' FORMAT JSON"
    )
  }

  it should "overrule with left preference" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from OneTestTable where col2 >= 2
    val composed = query :+> query2
    clickhouseTokenizer.toSql(composed.internalQuery) should be (
      s"SELECT shield_id FROM $tokenizerDatabase.captainAmerica WHERE column_2 >= 2 FORMAT JSON"
    )
  }

  it should "overrule with right preference" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from OneTestTable where col2 >= 2
    val composed = query <+: query2
    clickhouseTokenizer.toSql(composed.internalQuery) should be (
      s"SELECT item_id FROM $tokenizerDatabase.captainAmerica WHERE column_2 >= 2 FORMAT JSON"
    )
  }

  it should "fail on try override of conflicting queries" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from OneTestTable where col2 >= 2
    val composed = query + query2
    composed should matchPattern {
      case Failure(_:IllegalArgumentException) =>
    }
  }

  it should "succeed on safe override of non-conflicting multi part queries" in {
    val query = select(shieldId)
    val query2 = from(OneTestTable)
    val query3 = where(col2 >= 4)

    val composed = query + query2
    val composed2 = composed + query3

    composed should matchPattern {
      case t: Success[_] =>
    }

    clickhouseTokenizer.toSql(composed.get.internalQuery) should be (
      "SELECT shield_id FROM default.captainAmerica FORMAT JSON"
    )

    composed2 should matchPattern {
      case t: Success[_] =>
    }
    clickhouseTokenizer.toSql(composed2.get.internalQuery) should be (
      "SELECT shield_id FROM default.captainAmerica WHERE column_2 >= 4 FORMAT JSON"
    )
  }

  it should "throw an exception if the union doesn't have the same number of columns" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(shieldId, itemId) from OneTestTable

    an[IllegalArgumentException] should be thrownBy  {
      query.unionAll(query2)
    }
  }

  it should "properly tokenize conditional comparison statements with regular columns" in {
    val query = select(shieldId) from OneTestTable where ((shieldId.isEq(0) and col1 >= 1) >= 1)
    val generatedSql = clickhouseTokenizer.toSql(query.internalQuery)

    generatedSql should be (
      s"SELECT shield_id FROM $tokenizerDatabase.captainAmerica "
    )
  }


  it should "perform the union of multiple tables" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from TwoTestTable
    val query3 = select(itemId) from ThreeTestTable
    val union = query.unionAll(query2).unionAll(query3)
    val generatedSql = clickhouseTokenizer.toSql(union.internalQuery)

    generatedSql should be (
      s"SELECT shield_id FROM $tokenizerDatabase.captainAmerica UNION ALL SELECT item_id FROM $tokenizerDatabase.twoTestTable UNION ALL SELECT item_id FROM $tokenizerDatabase.threeTestTable FORMAT JSON"
    )
  }

  it should "select from an union of two tables" in {
    val query2 = select(itemId) from TwoTestTable
    val query3 = select(itemId) from ThreeTestTable
    val query = select(itemId) from query2.unionAll(query3)

    val generatedSql = clickhouseTokenizer.toSql(query.internalQuery)

    generatedSql should be (
      s"SELECT item_id FROM (SELECT item_id FROM $tokenizerDatabase.twoTestTable UNION ALL SELECT item_id FROM $tokenizerDatabase.threeTestTable) FORMAT JSON"
    )
  }
}
