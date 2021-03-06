package com.crobox.clickhouse.dsl.language

import java.util.UUID

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.testkit.ClickhouseMatchers
import com.crobox.clickhouse.time.{MultiDuration, MultiInterval, TimeUnit}
import org.joda.time.{DateTime, DateTimeZone}

class ClickhouseTokenizerTest
    extends ClickhouseClientSpec
    with TestSchema
    with ClickhouseTokenizerModule
    with ClickhouseMatchers {
  val testSubject = this
  val database    = "default"

  it should "build select statement" in {
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

  it should "add simple condition between columns" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                    Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                    where = Some(shieldId < itemId))
    )
    query should be("SELECT shield_id FROM default.captainAmerica WHERE shield_id < item_id FORMAT JSON")
  }

  it should "add condition for value" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query =
      testSubject.toSql(
        InternalQuery(Some(select),
                      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                      where = Some(shieldId < uuid))
      )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  it should "add chained condition" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                    Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                    where = Some(shieldId < uuid and shieldId < itemId))
    )
    query should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' AND shield_id < item_id FORMAT JSON"
    )
  }

  it should "group by alias if using aliased column" in {
    val alias  = shieldId as "preferable"
    val select = SelectQuery(Seq(alias))
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                    Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                    groupBy = Some(GroupByQuery(Seq(alias))))
    )
    query should be("SELECT shield_id AS preferable FROM default.captainAmerica GROUP BY preferable FORMAT JSON")
  }

  it should "group by with rollup if using group by mode" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        groupBy = Some(GroupByQuery(Seq(shieldId), mode = Some(GroupByQuery.WithRollup), withTotals = true))
      )
    )
    query should be(
      "SELECT shield_id FROM default.captainAmerica GROUP BY shield_id WITH ROLLUP WITH TOTALS FORMAT JSON"
    )
  }

  it should "group by with cube if using group by mode" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                    Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                    groupBy = Some(GroupByQuery(mode = Some(GroupByQuery.WithCube))))
    )
    query should be("SELECT shield_id FROM default.captainAmerica WITH CUBE FORMAT JSON")
  }

  it should "build table join using select all style" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        join = Some(
          JoinQuery(JoinQuery.InnerJoin, TableFromQuery[OneTestTable.type](OneTestTable), using = Seq(shieldId))
        )
      )
    )
    query should matchSQL(
      s"""
         | SELECT shield_id FROM default.captainAmerica AS l1
         | INNER JOIN (SELECT * FROM default.captainAmerica) AS r1 USING shield_id FORMAT JSON
         | """.stripMargin
    )
  }

  it should "use functions in group by method" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        orderBy = Seq((lower(shieldId), ASC))
      )
    )
    query should matchSQL(s"SELECT shield_id FROM default.captainAmerica ORDER BY lower(shield_id) ASC FORMAT JSON")
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
            using = Seq(shieldId),
            global = true
          )
        )
      )
    )
    query should matchSQL(
      s"""
         | SELECT shield_id FROM default.captainAmerica AS l1 GLOBAL
         | ANY LEFT JOIN (SELECT item_id AS shield_id FROM default.twoTestTable) AS r1 USING shield_id
         | FORMAT JSON""".stripMargin
    )
  }

  it should "generate CONDITIONAL cases" in {
    this.tokenizeColumn(switch(const(3)))(TokenizeContext()) shouldBe "3"
    this.tokenizeColumn(switch(shieldId, columnCase(col1.isEq("test"), itemId)))(TokenizeContext()) shouldBe
    (s"CASE WHEN ${col1.name} = 'test' THEN ${itemId.name} ELSE ${shieldId.name} END")
  }

  it should "generate CONDITIONAL multiIf" in {

    // test no cases
    this.tokenizeColumn(multiIf(const(3)))(TokenizeContext()) shouldBe "3"

    // test single case
    this.tokenizeColumn(multiIf(shieldId, columnCase(col1.isEq("test"), itemId)))(TokenizeContext()) shouldBe
    (s"if(${col1.name} = 'test', ${itemId.name}, ${shieldId.name})")

    // test multi cases
    this.tokenizeColumn(
      multiIf(shieldId, columnCase(col1.isEq("test"), itemId), columnCase(col1.isEq("test"), itemId))
    )(TokenizeContext()) shouldBe
    (s"multiIf(${col1.name} = 'test', ${itemId.name}, ${col1.name} = 'test', ${itemId.name}, ${shieldId.name})")
  }

  it should "use constant" in {
    this.tokenizeColumn(const(3).as(col2))(TokenizeContext()) shouldBe s"3 AS ${col2.name}"
  }

  "raw()" should "allow to behave like little bobby tables" in {
    val col    = RawColumn("Robert'); DROP TABLE students;")
    val select = SelectQuery(Seq(col))
    val query = testSubject.toSql(
      InternalQuery(
        select = Some(select),
        where = Some(col)
      )
    )
    query should be(s"SELECT ${col.rawSql} WHERE ${col.rawSql} FORMAT JSON")
  }

  "Aggregated functions" should "build with combinators" in {
    implicit val ctx = TokenizeContext()
    this.tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), uniq(col1))) shouldBe s"uniqIf(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), uniqHLL12(col1))) shouldBe s"uniqHLL12If(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), uniqCombined(col1))) shouldBe s"uniqCombinedIf(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(
      CombinedAggregatedFunction(Combinator.If(col1.isEq("test")),
                                 CombinedAggregatedFunction(Combinator.If(col2.isEq(3)), uniqExact(col1)))
    ) shouldBe s"uniqExactIfIf(${col1.name},${col2.name} = 3,${col1.name} = 'test')"
  }

  "Aggregated functions" should "uniq for multiple columns" in {
    implicit val ctx = TokenizeContext()
    this.tokenizeColumn(uniq(col1, col2)) shouldBe s"uniq(${col1.name},${col2.name})"
    this.tokenizeColumn(uniqHLL12(col1, col2)) shouldBe s"uniqHLL12(${col1.name},${col2.name})"
    this.tokenizeColumn(uniqExact(col1, col2)) shouldBe s"uniqExact(${col1.name},${col2.name})"
    this.tokenizeColumn(uniqCombined(col1, col2)) shouldBe s"uniqCombined(${col1.name},${col2.name})"

  }

  "build time series" should "use zone name for monthly" in {
    this.tokenizeTimeSeries(
      TimeSeries(
        timestampColumn,
        MultiInterval(DateTime.now(DateTimeZone.forOffsetHours(2)),
                      DateTime.now(DateTimeZone.forOffsetHours(2)),
                      MultiDuration(TimeUnit.Month))
      )
    )(TokenizeContext()) shouldBe "toDateTime(toStartOfMonth(toDateTime(ts / 1000), 'Etc/GMT-2'), 'Etc/GMT-2')"
  }

  "build custom refs" should "quote them correctly" in {
    val name   = "props.key"
    val col    = RefColumn(name)
    val select = SelectQuery(Seq(col))
    val query = testSubject.toSql(
      InternalQuery(
        select = Some(select)
      )
    )
    query should be(
      s"SELECT `$name` FORMAT JSON"
    )
  }
}
