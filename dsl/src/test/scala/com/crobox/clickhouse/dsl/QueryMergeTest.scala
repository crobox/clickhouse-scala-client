package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.dsl.parallel._
import com.crobox.clickhouse.{ClickhouseClientSpec, dsl => CHDsl}

class QueryMergeTest extends ClickhouseClientSpec with TestSchema {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}
  val database            = "query_merge"

  "query merge operators" should "collect columns from a right hand query" in {
    val expectedUUID = UUID.randomUUID()

    val left: OperationalQuery  = select(itemId) from TwoTestTable where (col3 isEq "wompalama")
    val right: OperationalQuery = select(CHDsl.all()) from OneTestTable where shieldId.isEq(expectedUUID)
    val query                   = right.merge(left) on timestampColumn

    // PURE SPECULATIVE / SQL ONLY
    // THE REASON WHY IT'S NOT --> ON twoTestTable.ts is that twoTestTable DOESN'T have a ts column.
    clickhouseTokenizer.toSql(query.internalQuery).replaceAll("[\\s\\n]", "") should be(
      s"""SELECT shield_id,numbers, * FROM (
         |  SELECT item_id, ts FROM $database.twoTestTable WHERE column_3 = 'wompalama' GROUP BY ts ORDER BY ts ASC
         |) ALL LEFT JOIN (
         |  SELECT * FROM $database.captainAmerica WHERE shield_id = '$expectedUUID' GROUP BY ts ORDER BY ts ASC
         |) AS TTT ON ts = TTT.ts FORMAT JSON""".stripMargin
        .replaceAll("[\\s\\n]", "")
    )
  }

  it should "recursively collect columns from right hand queries" in {
    val expectedUUID             = UUID.randomUUID()
    val left: OperationalQuery   = select(CHDsl.all()) from OneTestTable where shieldId.isEq(expectedUUID)
    val right: OperationalQuery  = select(CHDsl.all()) from TwoTestTable where (col3 isEq "wompalama")
    val right2: OperationalQuery = select(CHDsl.all()) from ThreeTestTable where shieldId.isEq(expectedUUID)
    val query                    = right2 merge (right) on timestampColumn merge (left) on timestampColumn
    val parsed                   = clickhouseTokenizer.toSql(query.internalQuery).replaceAll("[\\s\\n]", "")

    // PURE SPECULATIVE / SQL ONLY
    // THE REASON WHY IT'S NOT --> ON twoTestTable.ts is that twoTestTable DOESN'T have a ts column.
    parsed should equal(
      s"""SELECT item_id, column_1, column_2, column_3, column_4, column_5, column_6, * FROM (
         |  SELECT * FROM $database.captainAmerica WHERE shield_id = '$expectedUUID' GROUP BY ts ORDER BY ts ASC
         |) ALL LEFT JOIN (
         |  SELECT item_id, column_2, column_4, column_5, column_6, * FROM (
         |    SELECT * FROM $database.twoTestTable WHERE column_3 = 'wompalama' GROUP BY ts ORDER BY ts ASC
         |  ) ALL LEFT JOIN (
         |    SELECT * FROM $database.threeTestTable WHERE shield_id = '$expectedUUID' GROUP BY ts ORDER BY ts ASC
         |  ) AS TT1 ON ts = TT1.ts GROUP BY ts ORDER BY ts ASC
         |) AS TT2 ON ts = TT2.ts FORMAT JSON""".stripMargin
        .replaceAll("[\\s\\n]", "")
    )
  }

}
