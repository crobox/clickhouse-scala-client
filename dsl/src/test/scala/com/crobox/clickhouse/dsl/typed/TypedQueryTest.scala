package com.crobox.clickhouse.dsl.typed

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

/**
 * The typed select list is a contract: the decoder reads columns by position, so anything that widens the select list
 * after the fact breaks decoding at query time. These pin the width for the combinators that are reachable on a
 * [[TypedQuery]].
 */
class TypedQueryTest extends DslTestSpec {

  it should "not add the grouping column to the select list" in {
    val query = typed.select(count()) from OneTestTable groupBy shieldId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT count() FROM $database.captainAmerica GROUP BY shield_id FORMAT JSON"
    )
    query.decoder.arity shouldBe 1
  }

  it should "not add the ordering column to the select list" in {
    val query = typed.select(shieldId) from OneTestTable orderBy numbers
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica ORDER BY numbers ASC FORMAT JSON"
    )
    query.decoder.arity shouldBe 1
  }

  it should "not add the ordering column when a direction is given" in {
    val query = typed.select(shieldId) from OneTestTable orderByWithDirection ((numbers, DESC))
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica ORDER BY numbers DESC FORMAT JSON"
    )
    query.decoder.arity shouldBe 1
  }

  it should "accumulate grouping and ordering columns without touching the select list" in {
    val query = typed
      .select(count())
      .from(OneTestTable)
      .groupBy(shieldId)
      .groupBy(numbers)
      .orderBy(shieldId)
      .orderBy(numbers)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT count() FROM $database.captainAmerica GROUP BY shield_id, numbers " +
        "ORDER BY shield_id ASC, numbers ASC FORMAT JSON"
    )
    query.decoder.arity shouldBe 1
  }

  it should "keep the select list stable when the ordering column is already selected" in {
    val query = typed.select(shieldId) from OneTestTable orderBy shieldId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica ORDER BY shield_id ASC FORMAT JSON"
    )
    query.decoder.arity shouldBe 1
  }

  it should "keep the untyped groupBy merging behaviour untouched" in {
    val query = select(count()) from OneTestTable groupBy shieldId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT count(), shield_id FROM $database.captainAmerica GROUP BY shield_id FORMAT JSON"
    )
  }
}
