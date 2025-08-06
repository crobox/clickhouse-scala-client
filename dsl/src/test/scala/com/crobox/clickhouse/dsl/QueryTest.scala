package com.crobox.clickhouse.dsl

import com.crobox.clickhouse._
import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import org.joda.time.{DateTime, LocalDate}

import java.util.UUID
import scala.util.{Failure, Success}

class QueryTest extends DslTestSpec {

  it should "perform simple select" in {
    val query = select(shieldId) from OneTestTable
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "generate for join between tables" in {
    val query = select(col1, shieldId).from(OneTestTable).join(InnerJoin, TwoTestTable) using shieldId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT column_1, shield_id FROM $database.captainAmerica AS L1 INNER JOIN (SELECT * " +
        s"FROM $database.twoTestTable) AS R1 USING shield_id FORMAT JSON"
    )
  }

  it should "generate inner join" in {
    val expectedUUID                 = UUID.randomUUID()
    val innerQuery: OperationalQuery = select(shieldId as itemId) from OneTestTable where shieldId.isEq(expectedUUID)
    val joinInnerQuery: OperationalQuery = select(itemId) from TwoTestTable where (col3 isEq "wompalama")
    val query = select(col1, shieldId) from innerQuery join (InnerJoin, joinInnerQuery) using itemId
    toSql(query.internalQuery) should matchSQL(
      s"SELECT column_1, shield_id FROM (SELECT shield_id AS item_id FROM $database.captainAmerica " +
        s"WHERE shield_id = '$expectedUUID') AS L1 INNER JOIN (SELECT item_id FROM $database.twoTestTable " +
        s"WHERE column_3 = 'wompalama') AS R1 USING item_id FORMAT JSON"
    )
  }

  it should "escape from evil" in {
    val query = select(shieldId) from OneTestTable where col3.isEq("use ' evil")
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica WHERE column_3 = 'use \\' evil' FORMAT JSON"
    )
  }

  it should "overrule with left preference" in {
    val query    = select(shieldId) from OneTestTable
    val query2   = select(itemId) from OneTestTable where col2 >= 2
    val composed = query :+> query2
    toSql(composed.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica WHERE column_2 >= 2 FORMAT JSON"
    )
  }

  it should "overrule with right preference" in {
    val query    = select(shieldId) from OneTestTable
    val query2   = select(itemId) from OneTestTable where col2 >= 2
    val composed = query <+: query2
    toSql(composed.internalQuery) should matchSQL(
      s"SELECT item_id FROM $database.captainAmerica WHERE column_2 >= 2 FORMAT JSON"
    )
  }

  it should "compose indexOf and arrayElement" in {

    def lookupNestedValue(column: NativeColumn[_], elm: String): ExpressionColumn[String] =
      column.clickhouseType match {
        case ColumnType.Nested(k, v) =>
          val keyColumn   = ref[Seq[String]](column.name + "." + k.name)
          val valueColumn = ref[Seq[String]](column.name + "." + v.name)
          arrayElement(valueColumn, indexOf(keyColumn, elm))
        case _ =>
          throw new IllegalArgumentException(s"ColumnType ${column.clickhouseType} is unsupported for nested lookup")
      }

    val nested = NativeColumn("props", ColumnType.Nested(NativeColumn("key"), NativeColumn("value")))
    toSql(
      select(lookupNestedValue(nested, "cate'gory")).internalQuery
    ) should matchSQL(
      "SELECT `props.value`[indexOf(`props.key`, 'cate\\'gory')] FORMAT JSON"
    )

  }

  it should "fail on try override of conflicting queries" in {
    val query    = select(shieldId) from OneTestTable
    val query2   = select(itemId) from OneTestTable where col2 >= 2
    val composed = query + query2
    composed should matchPattern { case Failure(_: IllegalArgumentException) =>
    }
  }

  it should "parse datefunction" in {
    val query = select(toYear(NativeColumn[DateTime]("dateTime"))) from OneTestTable
    toSql(query.internalQuery).nonEmpty shouldBe true
  }

  it should "parse column function in filter" in {

    val query =
      select(minus(NativeColumn[LocalDate]("date"), NativeColumn[Double]("double"))) from OneTestTable where (sum(
        col2
      ) > 0)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT date - double FROM $database.captainAmerica WHERE sum(column_2) > 0 FORMAT JSON"
    )
  }

  it should "parse const as column for magnets" in {
    val query = select(col2 - 1, intDiv(2, 3)) from OneTestTable
    toSql(query.internalQuery) should matchSQL(
      s"SELECT column_2 - 1, intDiv(2, 3) FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "succeed on safe override of non-conflicting multi part queries" in {
    val query  = select(shieldId)
    val query2 = from(OneTestTable)
    val query3 = where(col2 >= 4)

    val composed  = query + query2
    val composed2 = composed + query3

    composed should matchPattern { case t: Success[_] =>
    }

    toSql(composed.get.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica FORMAT JSON"
    )

    composed2 should matchPattern { case t: Success[_] =>
    }
    toSql(composed2.get.internalQuery) should matchSQL(
      s"SELECT shield_id FROM $database.captainAmerica WHERE column_2 >= 4 FORMAT JSON"
    )
  }

  it should "throw an exception if the union doesn't have the same number of columns" in {
    val query  = select(shieldId) from OneTestTable
    val query2 = select(shieldId, itemId) from OneTestTable

    an[IllegalArgumentException] should be thrownBy
    query.unionAll(query2)
  }

  it should "perform the union of multiple tables" in {
    val query  = select(shieldId) from OneTestTable
    val query2 = select(itemId) from TwoTestTable
    val query3 = select(itemId) from ThreeTestTable
    val union  = query.unionAll(query2).unionAll(query3)
    toSql(union.internalQuery) should matchSQL(
      s"""
         |SELECT shield_id FROM $database.captainAmerica
         |UNION ALL SELECT item_id FROM $database.twoTestTable
         |UNION ALL SELECT item_id FROM $database.threeTestTable
         |FORMAT JSON""".stripMargin
    )
  }

  it should "select from an union of two tables" in {
    val query2 = select(itemId) from TwoTestTable
    val query3 = select(itemId) from ThreeTestTable
    val query  = select(itemId) from query2.unionAll(query3)

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT item_id FROM (SELECT item_id FROM $database.twoTestTable
         |UNION ALL SELECT item_id FROM $database.threeTestTable)
         |FORMAT JSON""".stripMargin
    )
  }

  it should "use alias in subselect" in {
    val query =
      select(dsl.all).from(select(col1, shieldId).from(OneTestTable).join(InnerJoin, TwoTestTable) using shieldId)
    toSql(query.internalQuery) should matchSQL(s"""
         |SELECT * FROM
         |(SELECT column_1, shield_id FROM $database.captainAmerica AS L1
         |  INNER JOIN (SELECT * FROM $database.twoTestTable) AS R1
         |  USING shield_id)
         |FORMAT JSON""".stripMargin)
  }

  it should "select from using ALIAS and final" in {
    var query = select(shieldId as itemId, col1, notEmpty(col1) as "empty").from(OneTestTable).as("3sf").asFinal

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT shield_id AS item_id, column_1, notEmpty(column_1) AS empty
         |FROM ${OneTestTable.quoted} AS `3sf` FINAL
         |FORMAT JSON""".stripMargin
    )

    query = select(shieldId as itemId, col1, notEmpty(col1) as "empty").from(OneTestTable).as("3sf")

    toSql(query.internalQuery) should matchSQL(
      s"""
         |SELECT shield_id AS item_id, column_1, notEmpty(column_1) AS empty
         |FROM ${OneTestTable.quoted} AS `3sf`
         |FORMAT JSON""".stripMargin
    )
  }

  it should "use distinct in select" in {
    val query =
      select(dsl.all).from(distinct(col1, shieldId).from(OneTestTable).join(InnerJoin, TwoTestTable) using shieldId)
    toSql(query.internalQuery) should matchSQL(s"""
                                                  |SELECT * FROM
                                                  |(SELECT DISTINCT column_1, shield_id FROM $database.captainAmerica AS L1
                                                  |  INNER JOIN (SELECT * FROM $database.twoTestTable) AS R1
                                                  |  USING shield_id)
                                                  |FORMAT JSON""".stripMargin)
  }

  it should "use distinct on in select" in {
    val query =
      select(dsl.all).from(
        distinctOn(col1)(col1, shieldId).from(OneTestTable).join(InnerJoin, TwoTestTable) using shieldId
      )
    toSql(query.internalQuery) should matchSQL(s"""
                                                  |SELECT * FROM
                                                  |(SELECT DISTINCT ON (column_1) column_1, shield_id FROM $database.captainAmerica AS L1
                                                  |  INNER JOIN (SELECT * FROM $database.twoTestTable) AS R1
                                                  |  USING shield_id)
                                                  |FORMAT JSON""".stripMargin)
  }

  it should "use distinct on in select tokenize injected sql" in {
    val col1SqlInjected = NativeColumn[String](s"); DROP TABLE $database.captainAmerica;(")
    val query           =
      select(dsl.all).from(
        distinctOn(col1SqlInjected)(col1, shieldId).from(OneTestTable).join(InnerJoin, TwoTestTable) using shieldId
      )
    toSql(query.internalQuery) should matchSQL(s"""
                                                  |SELECT * FROM
                                                  |(SELECT DISTINCT ON (`); DROP TABLE test.captainAmerica;(`) column_1, shield_id FROM $database.captainAmerica AS L1
                                                  |  INNER JOIN (SELECT * FROM $database.twoTestTable) AS R1
                                                  |  USING shield_id)
                                                  |FORMAT JSON""".stripMargin)
  }

  it should "generate LIMIT BY with single column" in {
    val query = select(shieldId, col1) from OneTestTable limitBy (2, shieldId)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id, column_1 FROM $database.captainAmerica LIMIT 2 BY shield_id FORMAT JSON"
    )
  }

  it should "generate LIMIT BY with multiple columns" in {
    val query = select(shieldId, col1, col2) from OneTestTable limitBy (3, shieldId, col1)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id, column_1, column_2 FROM $database.captainAmerica LIMIT 3 BY shield_id, column_1 FORMAT JSON"
    )
  }

  it should "generate LIMIT BY with offset" in {
    val query = select(shieldId, col1) from OneTestTable limitBy (5, 2, shieldId)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id, column_1 FROM $database.captainAmerica LIMIT 2, 5 BY shield_id FORMAT JSON"
    )
  }

  it should "generate LIMIT BY with complex query" in {
    val query = select(shieldId, col1, col2)
      .from(OneTestTable)
      .where(col2 > 10)
      .orderBy(col1)
      .limitBy(2, shieldId)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id, column_1, column_2 FROM $database.captainAmerica WHERE column_2 > 10 ORDER BY column_1 ASC LIMIT 2 BY shield_id FORMAT JSON"
    )
  }

  it should "generate LIMIT BY with LIMIT clause" in {
    val query = select(shieldId, col1)
      .from(OneTestTable)
      .limit(Some(Limit(100, 10)))
      .limitBy(2, shieldId)
    toSql(query.internalQuery) should matchSQL(
      s"SELECT shield_id, column_1 FROM $database.captainAmerica LIMIT 2 BY shield_id LIMIT 10, 100 FORMAT JSON"
    )
  }
}
