package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class ArrayJoinQueryTest extends DslTestSpec with TableDrivenPropertyChecks {

  it should "perform ARRAY JOIN on a single array column" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .arrayJoin(numbers)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM ${OneTestTable.quoted} ARRAY JOIN numbers FORMAT JSON"
    )
  }

  it should "perform LEFT ARRAY JOIN on a single array column" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .leftArrayJoin(numbers)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM ${OneTestTable.quoted} LEFT ARRAY JOIN numbers FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with alias" in {
    val aliasColumn = numbers as "num"
    val query = select(itemId, aliasColumn)
      .from(OneTestTable)
      .arrayJoin(aliasColumn)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS num FROM ${OneTestTable.quoted} ARRAY JOIN numbers AS num FORMAT JSON"
    )
  }

  it should "perform LEFT ARRAY JOIN with alias" in {
    val aliasColumn = numbers as "num"
    val query = select(itemId, aliasColumn)
      .from(OneTestTable)
      .leftArrayJoin(aliasColumn)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS num FROM ${OneTestTable.quoted} LEFT ARRAY JOIN numbers AS num FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN on multiple array columns" in {
    // Assume we have a table with multiple array columns for this test
    val numbersAlias = numbers as "nums1"
    val query = select(itemId, numbersAlias, numbers)
      .from(OneTestTable)
      .arrayJoin(numbersAlias, numbers)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS nums1, numbers FROM ${OneTestTable.quoted} ARRAY JOIN numbers AS nums1, numbers FORMAT JSON"
    )
  }

  it should "perform LEFT ARRAY JOIN on multiple array columns" in {
    val numbersAlias = numbers as "nums1"
    val query = select(itemId, numbersAlias, numbers)
      .from(OneTestTable)
      .leftArrayJoin(numbersAlias, numbers)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS nums1, numbers FROM ${OneTestTable.quoted} LEFT ARRAY JOIN numbers AS nums1, numbers FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with WHERE clause" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .where(itemId === "test")
      .arrayJoin(numbers)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM ${OneTestTable.quoted} ARRAY JOIN numbers WHERE item_id = 'test' FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with ORDER BY" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .arrayJoin(numbers)
      .orderByWithDirection((itemId, ASC), (numbers, DESC))
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM ${OneTestTable.quoted} ARRAY JOIN numbers ORDER BY item_id ASC, numbers DESC FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with LIMIT" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .arrayJoin(numbers)
      .limit(10)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM ${OneTestTable.quoted} ARRAY JOIN numbers LIMIT 10 FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with subquery" in {
    val subQuery = select(itemId, numbers).from(OneTestTable).where(notEmpty(numbers))
    val query = select(itemId, numbers)
      .from(subQuery)
      .arrayJoin(numbers)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM (SELECT item_id, numbers FROM ${OneTestTable.quoted} WHERE notEmpty(numbers)) AS L1 ARRAY JOIN numbers FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with GROUP BY on expanded elements" in {
    val query = select(itemId, count())
      .from(OneTestTable)
      .arrayJoin(numbers)
      .groupBy(itemId)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, count() FROM ${OneTestTable.quoted} ARRAY JOIN numbers GROUP BY item_id FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with JOIN on another table" in {
    val query = select(shieldId, numbers, col2)
      .from(OneTestTable)
      .arrayJoin(numbers)
      .join(JoinQuery.InnerJoin, TwoTestTable)
      .on((shieldId, "=", itemId))
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id, numbers, column_2 FROM ${OneTestTable.quoted} AS L1 ARRAY JOIN numbers INNER JOIN (SELECT * FROM ${TwoTestTable.quoted}) AS R1 ON L1.shield_id = R1.item_id FORMAT JSON"
    )
  }

  it should "fail when trying to combine ARRAY JOIN with regular JOIN in incompatible way" in {
    // This test should fail because ARRAY JOIN should come before other JOIN types
    // when generating SQL
    val query = select(shieldId, numbers, col2)
      .from(OneTestTable)
      .join(JoinQuery.InnerJoin, TwoTestTable)
      .on((shieldId, "=", itemId))
      .arrayJoin(numbers)
    
    // This should fail during SQL generation due to incorrect clause ordering
    an[Exception] shouldBe thrownBy(toSql(query.internalQuery))
  }

  it should "support complex ARRAY JOIN with multiple operations" in {
    val numbersWithAlias = numbers as "expanded_nums"
    val query = select(itemId, numbersWithAlias, count())
      .from(OneTestTable)
      .where(notEmpty(numbers))
      .arrayJoin(numbersWithAlias)
      .groupBy(itemId, numbersWithAlias)
      .having(count() > 1)
      .orderByWithDirection((count(), DESC))
      .limit(50)
    
    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS expanded_nums, count() FROM ${OneTestTable.quoted} " +
      s"ARRAY JOIN numbers AS expanded_nums " +
      s"WHERE notEmpty(numbers) " +
      s"GROUP BY item_id, expanded_nums " +
      s"HAVING count() > 1 " +
      s"ORDER BY count() DESC " +
      s"LIMIT 50 FORMAT JSON"
    )
  }
}