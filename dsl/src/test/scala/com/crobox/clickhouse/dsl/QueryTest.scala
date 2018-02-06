package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.dsl.JoinQuery.AnyInnerJoin
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._
import com.crobox.clickhouse.testkit.ClickhouseClientSpec

class QueryTest extends ClickhouseClientSpec with TestSchema {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}

  "querying using the typed query" should "perform simple select" in {
    val query = select(shieldId) from OneTestTable
    clickhouseTokenizer.toSql(query.underlying) should be(
      s"SELECT shield_id FROM $tokenizerDatabase.captainAmerica FORMAT JSON"
    )
  }

  it should "generate for join between tables" in {
    val query = select(col1, shieldId).from(OneTestTable).join(AnyInnerJoin, TwoTestTable) using shieldId
    clickhouseTokenizer.toSql(query.underlying) should be(
      s"SELECT column_1, shield_id FROM $tokenizerDatabase.captainAmerica ANY INNER JOIN (SELECT * FROM $tokenizerDatabase.twoTestTable ) USING shield_id FORMAT JSON"
    )
  }

  it should "generate inner join" in {
    val expectedUUID                     = UUID.randomUUID()
    val innerQuery: OperationalQuery     = select(shieldId as itemId) from OneTestTable where shieldId.isEq(expectedUUID)
    val joinInnerQuery: OperationalQuery = select(itemId) from TwoTestTable where (col3 isEq "wompalama")
    val query                            = select(col1, shieldId) from innerQuery join (AnyInnerJoin, joinInnerQuery) using itemId
    clickhouseTokenizer.toSql(query.underlying) should be(
      s"SELECT column_1, shield_id FROM (SELECT shield_id AS item_id FROM $tokenizerDatabase.captainAmerica WHERE shield_id = '$expectedUUID' ) ANY INNER JOIN (SELECT item_id FROM $tokenizerDatabase.twoTestTable WHERE column_3 = 'wompalama' ) USING item_id FORMAT JSON"
    )
  }

  it should "escape from evil" in {
    val query = select(shieldId) from OneTestTable where col3.isEq("use ' evil")
    clickhouseTokenizer.toSql(query.underlying) should be(
      s"SELECT shield_id FROM $tokenizerDatabase.captainAmerica WHERE column_3 = 'use \\' evil' FORMAT JSON"
    )
  }

}
