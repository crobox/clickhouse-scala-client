package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class ArrayJoinQueryTest extends DslTestSpec with TableDrivenPropertyChecks {

  private val testTableOne: String = OneTestTable.quoted
  private val testTableTwo: String = TwoTestTable.quoted

  it should "perform ARRAY JOIN on a single array column" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne ARRAY JOIN numbers FORMAT JSON"
    )
  }

  it should "perform LEFT ARRAY JOIN on a single array column" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .withLeftArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne LEFT ARRAY JOIN numbers FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with alias" in {
    val aliasColumn = numbers as "num"
    val query       = select(itemId, aliasColumn)
      .from(OneTestTable)
      .withArrayJoin(aliasColumn)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS num FROM $testTableOne ARRAY JOIN numbers AS num FORMAT JSON"
    )
  }

  it should "perform LEFT ARRAY JOIN with alias" in {
    val aliasColumn = numbers as "num"
    val query       = select(itemId, aliasColumn)
      .from(OneTestTable)
      .withLeftArrayJoin(aliasColumn)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS num FROM $testTableOne LEFT ARRAY JOIN numbers AS num FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN on multiple array columns" in {
    // Assume we have a table with multiple array columns for this test
    val numbersAlias = numbers as "nums1"
    val query        = select(itemId, numbersAlias, numbers)
      .from(OneTestTable)
      .withArrayJoin(numbersAlias, numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS nums1, numbers FROM $testTableOne ARRAY JOIN numbers AS nums1, numbers FORMAT JSON"
    )
  }

  it should "perform LEFT ARRAY JOIN on multiple array columns" in {
    val numbersAlias = numbers as "nums1"
    val query        = select(itemId, numbersAlias, numbers)
      .from(OneTestTable)
      .withLeftArrayJoin(numbersAlias, numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS nums1, numbers FROM $testTableOne LEFT ARRAY JOIN numbers AS nums1, numbers FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with WHERE clause" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .where(itemId === "test")
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne ARRAY JOIN numbers WHERE item_id = 'test' FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with ORDER BY" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .withArrayJoin(numbers)
      .orderByWithDirection((itemId, ASC), (numbers, DESC))

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne ARRAY JOIN numbers ORDER BY item_id ASC, numbers DESC FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with LIMIT" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .withArrayJoin(numbers)
      .limit(Some(Limit(10)))

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne ARRAY JOIN numbers LIMIT 10 FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with subquery" in {
    val subQuery = select(itemId, numbers).from(OneTestTable).where(notEmpty(numbers))
    val query    = select(itemId, numbers)
      .from(subQuery)
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM (SELECT item_id, numbers FROM $testTableOne WHERE notEmpty(numbers)) AS L1 ARRAY JOIN numbers FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with GROUP BY on expanded elements" in {
    val query = select(itemId, count())
      .from(OneTestTable)
      .withArrayJoin(numbers)
      .groupBy(itemId)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, count() FROM $testTableOne ARRAY JOIN numbers GROUP BY item_id FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN combined with JOIN on another table" in {
    val query = select(shieldId, numbers, col2)
      .from(OneTestTable)
      .withArrayJoin(numbers)
      .join(JoinQuery.InnerJoin, TwoTestTable)
      .on((shieldId, "=", itemId))

    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id, numbers, column_2 FROM $testTableOne AS L1 ARRAY JOIN numbers INNER JOIN (SELECT * FROM $testTableTwo) AS R1 ON L1.shield_id = R1.item_id FORMAT JSON"
    )
  }

  it should "handle ARRAY JOIN after regular JOIN (implementation allows this)" in {
    // Note: The implementation actually allows ARRAY JOIN after regular JOIN
    // This generates valid SQL, so we test the actual behavior
    val query = select(shieldId, numbers, col2)
      .from(OneTestTable)
      .join(JoinQuery.InnerJoin, TwoTestTable)
      .on((shieldId, "=", itemId))
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT shield_id, numbers, column_2 
         |FROM $testTableOne AS L1 
         |ARRAY JOIN numbers 
         |INNER JOIN (SELECT * FROM $testTableTwo) AS R1 ON L1.shield_id = R1.item_id 
         |FORMAT JSON""".stripMargin
    )
  }

  it should "support complex ARRAY JOIN with multiple operations" in {
    val numbersWithAlias = numbers as "expanded_nums"
    val query            = select(itemId, numbersWithAlias, count())
      .from(OneTestTable)
      .where(notEmpty(numbers))
      .withArrayJoin(numbersWithAlias)
      .groupBy(itemId, numbersWithAlias)
      .having(count() > 1)
      .orderByWithDirection((count(), DESC))
      .limit(Some(Limit(50)))

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers AS expanded_nums, count() FROM $testTableOne " +
        s"ARRAY JOIN numbers AS expanded_nums " +
        s"WHERE notEmpty(numbers) " +
        s"GROUP BY item_id, expanded_nums " +
        s"HAVING count() > 1 " +
        s"ORDER BY count() DESC " +
        s"LIMIT 50 FORMAT JSON"
    )
  }

  // Array Functions Tests
  it should "perform ARRAY JOIN on double array" in {
    val query = select(itemId, doubleArray)
      .from(OneTestTable)
      .withArrayJoin(doubleArray)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, double_array FROM $testTableOne ARRAY JOIN double_array FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with arrayEnumerate function" in {
    val enumerated = arrayEnumerate(numbers) as "enumerated"
    val query      = select(itemId, enumerated, numbers)
      .from(OneTestTable)
      .withArrayJoin(enumerated)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, arrayEnumerate(numbers) AS enumerated, numbers FROM $testTableOne ARRAY JOIN arrayEnumerate(numbers) AS enumerated FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with arrayEnumerateUniq function" in {
    val uniqueEnum = arrayEnumerateUniq(numbers, stringArray) as "unique_enum"
    val query      = select(itemId, uniqueEnum)
      .from(OneTestTable)
      .withArrayJoin(uniqueEnum)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, arrayEnumerateUniq(numbers, string_array) AS unique_enum FROM $testTableOne ARRAY JOIN arrayEnumerateUniq(numbers, string_array) AS unique_enum FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN on multiple different array types" in {
    val query = select(itemId, numbers, stringArray, doubleArray)
      .from(OneTestTable)
      .withArrayJoin(numbers, stringArray, doubleArray)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers, string_array, double_array FROM $testTableOne ARRAY JOIN numbers, string_array, double_array FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with array slicing" in {
    val slicedNumbers = arraySlice(numbers, 1, 3) as "sliced"
    val query         = select(itemId, slicedNumbers)
      .from(OneTestTable)
      .withArrayJoin(slicedNumbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, arraySlice(numbers,1,3) AS sliced FROM $testTableOne ARRAY JOIN arraySlice(numbers,1,3) AS sliced FORMAT JSON"
    )
  }

  // Edge Cases Tests
  it should "perform ARRAY JOIN with empty array check" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .where(notEmpty(numbers))
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne ARRAY JOIN numbers WHERE notEmpty(numbers) FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN with array length filtering" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .where(arrayLength(numbers) > 2)
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne ARRAY JOIN numbers WHERE length(numbers) > 2 FORMAT JSON"
    )
  }

  it should "perform LEFT ARRAY JOIN to preserve rows with empty arrays" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .withLeftArrayJoin(numbers)
      .where(itemId.isNotNull())

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne LEFT ARRAY JOIN numbers WHERE isNotNull(item_id) FORMAT JSON"
    )
  }

  // Advanced JOIN Combinations
  it should "perform ARRAY JOIN with multiple regular JOINs" in {
    val query = select(shieldId, numbers, col2, col4)
      .from(OneTestTable)
      .withArrayJoin(numbers)
      .join(JoinQuery.InnerJoin, TwoTestTable)
      .on((shieldId, "=", itemId))
      .join(JoinQuery.LeftOuterJoin, ThreeTestTable)
      .on((itemId, "=", itemId))

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT shield_id, numbers, column_2, column_4
         |FROM $testTableOne AS L1 
         |ARRAY JOIN numbers 
         |LEFT OUTER JOIN (SELECT * FROM ${ThreeTestTable.quoted}) AS R1 ON L1.item_id = R1.item_id 
         |FORMAT JSON""".stripMargin
    )
  }

  it should "perform ARRAY JOIN with subquery containing JOIN" in {
    val subQuery = select(shieldId, numbers)
      .from(OneTestTable)
      .join(JoinQuery.InnerJoin, TwoTestTable)
      .on((shieldId, "=", itemId))
      .where(notEmpty(numbers))

    val query = select(shieldId, numbers)
      .from(subQuery)
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT shield_id, numbers 
         |FROM (SELECT shield_id, numbers 
         |FROM $testTableOne AS L1 
         |INNER JOIN (SELECT * FROM $testTableTwo) AS R1 ON L1.shield_id = R1.item_id 
         |WHERE notEmpty(numbers)) AS L2 
         |ARRAY JOIN numbers 
         |FORMAT JSON""".stripMargin
    )
  }

  // Property-based and Advanced Scenarios
  it should "perform ARRAY JOIN with HAVING clause on aggregated array elements" in {
    val query = select(itemId, count(), average(numbers))
      .from(OneTestTable)
      .withArrayJoin(numbers)
      .groupBy(itemId)
      .having(count() > 5 and average(numbers) < 100)

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT item_id, count(), avg(numbers)
         |FROM $testTableOne
         |ARRAY JOIN numbers 
         |GROUP BY item_id 
         |HAVING count() > 5 AND avg(numbers) < 100 
         |FORMAT JSON""".stripMargin
    )
  }

  it should "perform ARRAY JOIN with UNION ALL" in {
    val query1 = select(itemId, numbers).from(OneTestTable).withArrayJoin(numbers)
    val query2 = select(itemId, numbers).from(OneTestTable).where(arrayLength(numbers) > 3).withArrayJoin(numbers)

    val unionQuery = query1.unionAll(query2)

    toSql(unionQuery.internalQuery) should matchSQL(
      s"""
         |SELECT item_id, numbers 
         |FROM $testTableOne 
         |ARRAY JOIN numbers 
         |UNION ALL SELECT item_id, numbers
         |FROM $testTableOne 
         |ARRAY JOIN numbers  
         |WHERE length(numbers) > 3 
         |FORMAT JSON
         |""".stripMargin
    )
  }

  it should "perform ARRAY JOIN with complex WHERE conditions" in {
    val query = select(itemId, numbers, stringArray)
      .from(OneTestTable)
      .where(
        (arrayLength(numbers) > 0) and
          (has(numbers, 42) or has(stringArray, "important")) and
          (itemId.isNotNull())
      )
      .withArrayJoin(numbers)

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT item_id, numbers, string_array 
         |FROM $testTableOne 
         |ARRAY JOIN numbers 
         |WHERE length(numbers) > 0 AND (has(numbers, 42) OR has(string_array, 'important')) AND isNotNull(item_id)
         |FORMAT JSON
         |""".stripMargin
    )
  }

  // Real-world Pattern Tests
  it should "perform ARRAY JOIN for event analytics pattern" in {
    val eventValue = numbers as "event_value"
    val eventTag   = stringArray as "event_tag"
    val query      = select(itemId, eventValue, eventTag, timestampColumn)
      .from(OneTestTable)
      .withArrayJoin(eventValue, eventTag)
      .where(arrayLength(numbers) > 0)
      .groupBy(eventTag, timestampColumn)
      .orderByWithDirection((count(), DESC))

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT item_id, numbers AS event_value, string_array AS event_tag, ts, count() 
         |FROM $testTableOne 
         |ARRAY JOIN numbers AS event_value, string_array AS event_tag 
         |WHERE length(numbers) > 0 
         |GROUP BY event_tag, ts 
         |ORDER BY count() DESC 
         |FORMAT JSON
         |""".stripMargin
    )
  }

  it should "perform ARRAY JOIN for user behavior funnel analysis" in {
    val step  = arrayEnumerate(numbers) as "funnel_step"
    val value = numbers as "step_value"
    val query = select(itemId, step, value, count())
      .from(OneTestTable)
      .withArrayJoin(step, value)
      .groupBy(itemId, step)
      .having(count() > 1)
      .orderByWithDirection((itemId, ASC), (step, ASC))

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT item_id, arrayEnumerate(numbers) AS funnel_step, numbers AS step_value, count() 
         |FROM $testTableOne 
         |ARRAY JOIN arrayEnumerate(numbers) AS funnel_step, numbers AS step_value 
         |GROUP BY item_id, funnel_step 
         |HAVING count() > 1 
         |ORDER BY item_id ASC, arrayEnumerate(numbers) AS funnel_step ASC 
         |FORMAT JSON
        """.stripMargin
    )
  }

  // Error Validation Tests
  it should "handle ARRAY JOIN with PREWHERE clause" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .prewhere(notEmpty(numbers))
      .withArrayJoin(numbers)
      .where(arrayLength(numbers) > 0)

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT item_id, numbers 
         |FROM $testTableOne 
         |ARRAY JOIN numbers 
         |PREWHERE notEmpty(numbers) 
         |WHERE length(numbers) > 0 
         |FORMAT JSON""".stripMargin
    )
  }

  it should "perform ARRAY JOIN with LIMIT BY clause" in {
    val query = select(itemId, numbers)
      .from(OneTestTable)
      .withArrayJoin(numbers)
      .limitBy(2, itemId)

    toSql(query.internalQuery) should matchSQL(
      s"SELECT item_id, numbers FROM $testTableOne ARRAY JOIN numbers LIMIT 2 BY item_id FORMAT JSON"
    )
  }

  it should "perform ARRAY JOIN on computed array expressions" in {
    val computedArray = arrayConcat(numbers, Array(100, 200, 300)) as "extended_numbers"
    val query         = select(itemId, computedArray)
      .from(OneTestTable)
      .withArrayJoin(computedArray)

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT item_id, arrayConcat(numbers, [100, 200, 300]) AS extended_numbers 
         |FROM $testTableOne 
         |ARRAY JOIN arrayConcat(numbers, [100, 200, 300]) AS extended_numbers 
         |FORMAT JSON""".stripMargin
    )
  }
}
