package com.crobox.clickhouse.dsl.clickhouse

import java.util.UUID

import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._
import com.crobox.clickhouse.dsl.{
  Case,
  ColumnOperations,
  Conditional,
  InnerJoinedQuery,
  JoinQuery,
  Limit,
  NoOpComparison,
  SelectQuery,
  TableColumn,
  TableFromQuery,
  TableJoinedQuery,
  TestSchema,
  InternalQuery
}
import com.crobox.clickhouse.testkit.ClickhouseClientSpec

import scala.collection.mutable

class ClickhouseTokenizerTest extends ClickhouseClientSpec with TestSchema with ClickhouseTokenizerModule {
  val testSubject = this

  "building select statement" should "build select statement" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val generatedSql =
      testSubject.toSql(InternalQuery(select, TableFromQuery[OneTestTable.type](select, OneTestTable)))
    generatedSql should be("SELECT shield_id FROM default.captainAmerica FORMAT JSON")
  }

  it should "build select with alias" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId as "preferable"))
    testSubject.toSql(InternalQuery(select, TableFromQuery[OneTestTable.type](select, OneTestTable))) should be(
      "SELECT shield_id AS preferable FROM default.captainAmerica FORMAT JSON"
    )
  }

  it should "use paging" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val generatedSql = testSubject.toSql(
      InternalQuery(select, TableFromQuery[OneTestTable.type](select, OneTestTable), limit = Some(Limit(15, 30)))
    )
    generatedSql should be("SELECT shield_id FROM default.captainAmerica LIMIT 30, 15 FORMAT JSON")

    val generatedSql2 = testSubject.toSql(
      InternalQuery(select, TableFromQuery[OneTestTable.type](select, OneTestTable), limit = Some(Limit(45)))
    )
    generatedSql2 should be("SELECT shield_id FROM default.captainAmerica LIMIT 0, 45 FORMAT JSON")
  }

  "building where clause" should "add simple condition between columns" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val query = testSubject.toSql(
      InternalQuery(select, TableFromQuery[OneTestTable.type](select, OneTestTable), Some(shieldId < itemId))
    )
    query should be("SELECT shield_id FROM default.captainAmerica WHERE shield_id < item_id FORMAT JSON")
  }

  it should "add condition for value" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val uuid   = UUID.randomUUID()
    val query =
      testSubject.toSql(
        InternalQuery(select, TableFromQuery[OneTestTable.type](select, OneTestTable), Some(shieldId < uuid))
      )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  it should "add chained condition" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(select,
                      TableFromQuery[OneTestTable.type](select, OneTestTable),
                      Some(shieldId < uuid and shieldId < itemId))
    )
    query should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' AND shield_id < item_id FORMAT JSON"
    )

  }

  it should "ignore noOp right condition" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(select,
                      TableFromQuery[OneTestTable.type](select, OneTestTable),
                      Some(shieldId < uuid and NoOpComparison()))
    )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  it should "ignore noOp left condition" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(select,
                      TableFromQuery[OneTestTable.type](select, OneTestTable),
                      Some(NoOpComparison() and shieldId < uuid))
    )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  "building group by" should "add columns as group by clauses" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val query = testSubject.toSql(
      InternalQuery(select,
                      TableFromQuery[OneTestTable.type](select, OneTestTable),
                      groupBy = mutable.LinkedHashSet(shieldId))
    )
    query should be("SELECT shield_id FROM default.captainAmerica GROUP BY shield_id FORMAT JSON")
  }

  it should "group by alias if using aliased column" in {
    val alias  = shieldId as "preferable"
    val select = SelectQuery(mutable.LinkedHashSet(alias))
    val query = testSubject.toSql(
      InternalQuery(select,
                      TableFromQuery[OneTestTable.type](select, OneTestTable),
                      groupBy = mutable.LinkedHashSet(alias))
    )
    query should be("SELECT shield_id AS preferable FROM default.captainAmerica GROUP BY preferable FORMAT JSON")
  }

  "building joins" should "build table join using select all style" in {
    val select = SelectQuery(mutable.LinkedHashSet(shieldId))
    val query = testSubject.toSql(
      InternalQuery(
        select,
        TableFromQuery[OneTestTable.type](select, OneTestTable),
        join = Some(TableJoinedQuery(select.internalQuery, `type` = JoinQuery.AnyInnerJoin, OneTestTable, Set(shieldId)))
      )
    )
    query should be(
      "SELECT shield_id FROM default.captainAmerica ANY INNER JOIN (SELECT * FROM default.captainAmerica ) USING shield_id FORMAT JSON"
    )
  }

  it should "use inner query as join" in {
    val select     = SelectQuery(mutable.LinkedHashSet(shieldId))
    val joinSelect = SelectQuery(mutable.LinkedHashSet(itemId as "shield_id"))
    val query = testSubject.toSql(
      InternalQuery(
        select,
        TableFromQuery[OneTestTable.type](select, OneTestTable),
        join = Some(
          InnerJoinedQuery(joinSelect.internalQuery,
                           JoinQuery.AnyLeftJoin,
                           TableFromQuery[TwoTestTable.type](joinSelect, TwoTestTable),
                           Set(shieldId))
        )
      )
    )
    query should be(
      "SELECT shield_id FROM default.captainAmerica ANY LEFT JOIN (SELECT item_id AS shield_id FROM default.twoTestTable ) USING shield_id FORMAT JSON"
    )
  }

  it should "generate higher order function" in {
    val col = new TableColumn[Seq[Int]]("table_column") {}
    this.tokenizeCondition(col.exists(_.isEq(3))) shouldBe "arrayExists(x -> x = 3, table_column)"
  }

  it should "generate cases" in {
    this.tokenizeColumn(Conditional(Seq(Case(itemId, col1.isEq("test"))), shieldId)) shouldBe s"CASE WHEN ${col1.name} = 'test' THEN ${itemId.name} ELSE ${shieldId.name} END"
  }

  it should "use constant" in {
    this.tokenizeColumn(
      ColumnOperations
        .const(3)
        .as(col2)
    ) shouldBe s"3 AS ${col2.name}"
  }
}
