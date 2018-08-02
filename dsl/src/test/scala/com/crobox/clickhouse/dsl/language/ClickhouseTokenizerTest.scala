package com.crobox.clickhouse.dsl.language

import java.util.UUID

import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._
import com.crobox.clickhouse.dsl.{
  AggregateFunction,
  Case,
  ColumnOperations,
  CombinedAggregatedFunction,
  Conditional,
  InnerFromQuery,
  InternalQuery,
  JoinQuery,
  Limit,
  NoOpComparison,
  OperationalQuery,
  SelectQuery,
  TableColumn,
  TableFromQuery,
  TestSchema,
  TimeSeries,
  Uniq
}
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import com.crobox.clickhouse.time.{MultiDuration, MultiInterval, TimeUnit}
import org.joda.time.{DateTime, DateTimeZone}

class ClickhouseTokenizerTest extends ClickhouseClientSpec with TestSchema with ClickhouseTokenizerModule {
  val testSubject = this

  "building select statement" should "build select statement" in {
    val select = SelectQuery(Seq(shieldId))
    val generatedSql =
      testSubject.toSql(InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable))))
    generatedSql should be("SELECT shield_id FROM default.captainAmerica FORMAT JSON")
  }

  it should "build select with alias" in {
    val select = SelectQuery(Seq(shieldId as "preferable"))
    testSubject.toSql(InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)))) should be(
      "SELECT shield_id AS preferable FROM default.captainAmerica FORMAT JSON"
    )
  }

  it should "use paging" in {
    val select = SelectQuery(Seq(shieldId))
    val generatedSql = testSubject.toSql(
      InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), limit = Some(Limit(15, 30)))
    )
    generatedSql should be("SELECT shield_id FROM default.captainAmerica LIMIT 30, 15 FORMAT JSON")

    val generatedSql2 = testSubject.toSql(
      InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), limit = Some(Limit(45)))
    )
    generatedSql2 should be("SELECT shield_id FROM default.captainAmerica LIMIT 0, 45 FORMAT JSON")
  }

  "building where clause" should "add simple condition between columns" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), false, Some(shieldId < itemId))
    )
    query should be("SELECT shield_id FROM default.captainAmerica WHERE shield_id < item_id FORMAT JSON")
  }

  it should "add condition for value" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query =
      testSubject.toSql(
        InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), false, Some(shieldId < uuid))
      )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  it should "add chained condition" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                      false,Some(shieldId < uuid and shieldId < itemId))
    )
    query should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' AND shield_id < item_id FORMAT JSON"
    )

  }

  it should "ignore noOp right condition" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                      false,Some(shieldId < uuid and NoOpComparison()))
    )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  it should "ignore noOp left condition" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                      false,Some(NoOpComparison() and shieldId < uuid))
    )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  "building group by" should "add columns as group by clauses" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), groupBy = Seq(shieldId))
    )
    query should be("SELECT shield_id FROM default.captainAmerica GROUP BY shield_id FORMAT JSON")
  }

  it should "group by alias if using aliased column" in {
    val alias  = shieldId as "preferable"
    val select = SelectQuery(Seq(alias))
    val query = testSubject.toSql(
      InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), groupBy = Seq(alias))
    )
    query should be("SELECT shield_id AS preferable FROM default.captainAmerica GROUP BY preferable FORMAT JSON")
  }

  "building joins" should "build table join using select all style" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        join = Some(JoinQuery(JoinQuery.AnyInnerJoin, TableFromQuery[OneTestTable.type](OneTestTable), Set(shieldId)))
      )
    )
    query should be(
      "SELECT shield_id FROM default.captainAmerica ANY INNER JOIN (SELECT * FROM default.captainAmerica) USING shield_id FORMAT JSON"
    )
  }

  it should "use inner query as join" in {
    val select     = SelectQuery(Seq(shieldId))
    val joinSelect = SelectQuery(Seq(itemId as "shield_id"))
    val query = testSubject.toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        join = Some(
          JoinQuery(
            JoinQuery.AnyLeftJoin,
            InnerFromQuery(
              OperationalQuery(InternalQuery(Some(joinSelect), Some(TableFromQuery[TwoTestTable.type](TwoTestTable))))
            ),
            Set(shieldId)
          )
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

  "Aggregated functions" should "build with combinators" in {
    this.tokenizeColumn(CombinedAggregatedFunction(AggregateFunction.If(col1.isEq("test")), Uniq(col1))) shouldBe s"uniqIf(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(CombinedAggregatedFunction(AggregateFunction.If(col1.isEq("test")), Uniq(col1, Uniq.HLL12))) shouldBe s"uniqHLL12If(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(CombinedAggregatedFunction(AggregateFunction.If(col1.isEq("test")), Uniq(col1, Uniq.Combined))) shouldBe s"uniqCombinedIf(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(
      CombinedAggregatedFunction(AggregateFunction.If(col1.isEq("test")),
                                 CombinedAggregatedFunction(AggregateFunction.If(col2.isEq(3)), Uniq(col1, Uniq.Exact)))
    ) shouldBe s"uniqExactIfIf(${col1.name},${col2.name} = 3,${col1.name} = 'test')"
  }

  "build time series" should "use zone name for monthly" in {
    this.tokenizeTimeSeries(
      TimeSeries(timestampColumn, MultiInterval(DateTime.now(DateTimeZone.forOffsetHours(2)),
                            DateTime.now(DateTimeZone.forOffsetHours(2)),
                            MultiDuration(TimeUnit.Month)))
    ) shouldBe "toDateTime(toStartOfMonth(toDateTime(ts / 1000), 'Africa/Maputo'), 'Africa/Maputo')"
  }
}
