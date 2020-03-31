package com.crobox.clickhouse.dsl.language

import java.util.UUID

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.time.{MultiDuration, MultiInterval, TimeUnit}
import org.joda.time.{DateTime, DateTimeZone}

class ClickhouseTokenizerTest extends ClickhouseClientSpec with TestSchema with ClickhouseTokenizerModule {
  val testSubject = this
  val database    = "default"

  def noto(other: LogicalOpsMagnet): ExpressionColumn[Boolean] = LogicalFunction(other, Not, other)

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
                    false,
                    None,
                    Some(shieldId < itemId))
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
                      false,
                      None,
                      Some(shieldId < uuid))
      )
    query should be(s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' FORMAT JSON")
  }

  it should "add chained condition" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                    Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                    false,
                    None,
                    Some(shieldId < uuid and shieldId < itemId))
    )
    query should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' AND shield_id < item_id FORMAT JSON"
    )
  }

  it should "add brackets between or/and (left double, right single)" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val internalQuery = InternalQuery(Some(select),
                                      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                                      false,
                                      None,
                                      Some((shieldId < uuid and (shieldId isEq itemId)) or shieldId < itemId))
    testSubject.toSql(
      internalQuery
    ) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE (shield_id < '$uuid' AND shield_id = item_id) OR shield_id < item_id FORMAT JSON"
    )
  }

  it should "add brackets between or/and (left double, right double)" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val internalQuery = InternalQuery(
      Some(select),
      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
      false,
      None,
      Some((shieldId < uuid and (shieldId isEq itemId)) or (shieldId < itemId or shieldId > "cro"))
    )
    testSubject.toSql(
      internalQuery
    ) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE (shield_id < '$uuid' AND shield_id = item_id) OR (shield_id < item_id OR shield_id > 'cro') FORMAT JSON"
    )
  }

  it should "add brackets in nested and/or" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val internalQuery = InternalQuery(
      Some(select),
      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
      false,
      None,
      Some(shieldId < uuid or ((shieldId isEq itemId) or (shieldId < itemId and shieldId > "cro")))
    )
    testSubject.toSql(
      internalQuery
    ) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE shield_id < '$uuid' OR (shield_id = item_id OR (shield_id < item_id AND shield_id > 'cro')) FORMAT JSON"
    )
  }

  it should "add brackets 1" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val internalQuery = InternalQuery(
      Some(select),
      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
      false,
      None,
      Some(shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c")))
    )
    testSubject.toSql(
      internalQuery
    ) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c') FORMAT JSON"
    )
  }

  it should "add brackets 2" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val internalQuery = InternalQuery(
      Some(select),
      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
      false,
      None,
      Some(
        shieldId isEq "a" or ((shieldId isEq "b") and (shieldId isEq "c")) or (noto(shieldId isEq "d") and noto(shieldId isEq "e"))
      )
    )
    testSubject.toSql(
      internalQuery
    ) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE (shield_id = 'a' OR (shield_id = 'b' AND shield_id = 'c')) OR (not(shield_id = 'd') AND not(shield_id = 'e')) FORMAT JSON"
    )
  }

  it should "add brackets 3 (not operator and OR)" in {
    val select = SelectQuery(Seq(shieldId))
    val internalQuery = InternalQuery(
      Some(select),
      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
      false,
      None,
      Some((noto(shieldId isEq "a")) or (noto(shieldId isEq "b")))
    )
    testSubject.toSql(
      internalQuery
    ) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE not(shield_id = 'a') OR not(shield_id = 'b') FORMAT JSON"
    )
  }

  it should "add brackets 3 (not operator and AND)" in {
    val select = SelectQuery(Seq(shieldId))
    val internalQuery = InternalQuery(
      Some(select),
      Some(TableFromQuery[OneTestTable.type](OneTestTable)),
      false,
      None,
      Some((noto(shieldId isEq "a")) and (noto(shieldId isEq "b")))
    )
    testSubject.toSql(
      internalQuery
    ) should be(
      s"SELECT shield_id FROM default.captainAmerica WHERE not(shield_id = 'a') AND not(shield_id = 'b') FORMAT JSON"
    )
  }

  it should "add columns as group by clauses" in {
    val select = SelectQuery(Seq(shieldId))
    val query = testSubject.toSql(
      InternalQuery(Some(select),
                    Some(TableFromQuery[OneTestTable.type](OneTestTable)),
                    groupBy = Some(GroupByQuery(Seq(shieldId))))
    )
    query should be("SELECT shield_id FROM default.captainAmerica GROUP BY shield_id FORMAT JSON")
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
        join = Some(JoinQuery(JoinQuery.AnyInnerJoin, TableFromQuery[OneTestTable.type](OneTestTable), Seq(shieldId)))
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
            Seq(shieldId),
            true
          )
        )
      )
    )
    query should be(
      "SELECT shield_id FROM default.captainAmerica GLOBAL ANY LEFT JOIN (SELECT item_id AS shield_id FROM default.twoTestTable ) USING shield_id FORMAT JSON"
    )
  }

  it should "generate higher order function" in {
    val col = new TableColumn[Iterable[Int]]("table_column") {}
    this.tokenizeColumn(arrayExists[Int]((x: TableColumn[Int]) => x.isEq(3), col)) shouldBe "arrayExists(x -> x = 3, table_column)"
  }

  it should "generate cases" in {
    this.tokenizeColumn(switch(const(3))) shouldBe "3"
    this.tokenizeColumn(switch(shieldId, columnCase(col1.isEq("test"), itemId))) shouldBe s"CASE WHEN ${col1.name} = 'test' THEN ${itemId.name} ELSE ${shieldId.name} END"
  }

  it should "use constant" in {
    this.tokenizeColumn(const(3).as(col2)) shouldBe s"3 AS ${col2.name}"
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
    query should be(
      s"SELECT ${col.rawSql} WHERE ${col.rawSql} FORMAT JSON"
    )
  }

  "Aggregated functions" should "build with combinators" in {
    this.tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), Uniq(col1))) shouldBe s"uniqIf(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), Uniq(col1, UniqModifier.HLL12))) shouldBe s"uniqHLL12If(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), Uniq(col1, UniqModifier.Combined))) shouldBe s"uniqCombinedIf(${col1.name},${col1.name} = 'test')"
    this.tokenizeColumn(
      CombinedAggregatedFunction(Combinator.If(col1.isEq("test")),
                                 CombinedAggregatedFunction(Combinator.If(col2.isEq(3)),
                                                            Uniq(col1, UniqModifier.Exact)))
    ) shouldBe s"uniqExactIfIf(${col1.name},${col2.name} = 3,${col1.name} = 'test')"
  }

  "build time series" should "use zone name for monthly" in {
    this.tokenizeTimeSeries(
      TimeSeries(
        timestampColumn,
        MultiInterval(DateTime.now(DateTimeZone.forOffsetHours(2)),
                      DateTime.now(DateTimeZone.forOffsetHours(2)),
                      MultiDuration(TimeUnit.Month))
      )
    ) shouldBe "toDateTime(toStartOfMonth(toDateTime(ts / 1000), 'Etc/GMT-2'), 'Etc/GMT-2')"
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
