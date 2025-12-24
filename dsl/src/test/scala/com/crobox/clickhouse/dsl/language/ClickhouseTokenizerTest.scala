package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.time.{MultiDuration, MultiInterval, TimeUnit}
import org.joda.time.{DateTime, DateTimeZone}

import java.util.UUID

class ClickhouseTokenizerTest extends DslTestSpec {

  it should "build select statement" in {
    val select       = SelectQuery(Seq(shieldId))
    val generatedSql = toSql(InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable))))
    generatedSql should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} FORMAT JSON")
  }

  it should "build select with alias" in {
    val select = SelectQuery(Seq(shieldId as "preferable"))
    toSql(InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)))) should matchSQL(
      s"SELECT shield_id AS preferable FROM ${OneTestTable.quoted} FORMAT JSON"
    )
  }

  it should "use paging" in {
    val select       = SelectQuery(Seq(shieldId))
    val generatedSql = toSql(
      InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), limit = Some(Limit(15, 30)))
    )
    generatedSql should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 30, 15 FORMAT JSON")

    val generatedSql2 = toSql(
      InternalQuery(Some(select), Some(TableFromQuery[OneTestTable.type](OneTestTable)), limit = Some(Limit(45)))
    )
    generatedSql2 should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} LIMIT 0, 45 FORMAT JSON")
  }

  it should "add simple condition between columns" in {
    val select = SelectQuery(Seq(shieldId))
    val query  = toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        where = Some(shieldId < itemId)
      )
    )
    query should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} WHERE shield_id < item_id FORMAT JSON")
  }

  it should "add condition for value" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query  =
      toSql(
        InternalQuery(
          Some(select),
          Some(TableFromQuery[OneTestTable.type](OneTestTable)),
          where = Some(shieldId < uuid)
        )
      )
    query should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} WHERE shield_id < '$uuid' FORMAT JSON")
  }

  it should "add chained condition" in {
    val select = SelectQuery(Seq(shieldId))
    val uuid   = UUID.randomUUID()
    val query  = toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        where = Some(shieldId < uuid and shieldId < itemId)
      )
    )
    query should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} WHERE shield_id < '$uuid' AND shield_id < item_id FORMAT JSON"
    )
  }

  it should "group by alias if using aliased column" in {
    val alias  = shieldId as "preferable"
    val select = SelectQuery(Seq(alias))
    val query  = toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        groupBy = Some(GroupByQuery(Seq(alias)))
      )
    )
    query should matchSQL(s"SELECT shield_id AS preferable FROM ${OneTestTable.quoted} GROUP BY preferable FORMAT JSON")
  }

  it should "group by with rollup if using group by mode" in {
    val select = SelectQuery(Seq(shieldId))
    val query  = toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        groupBy = Some(GroupByQuery(Seq(shieldId), mode = Some(GroupByQuery.WithRollup), withTotals = true))
      )
    )
    query should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted} GROUP BY shield_id WITH ROLLUP WITH TOTALS FORMAT JSON"
    )
  }

  it should "group by with cube if using group by mode" in {
    val select = SelectQuery(Seq(shieldId))
    val query  = toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        groupBy = Some(GroupByQuery(mode = Some(GroupByQuery.WithCube)))
      )
    )
    query should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} WITH CUBE FORMAT JSON")
  }

  it should "build table join using select all style" in {
    val select = SelectQuery(Seq(shieldId))
    val query  = toSql(
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
         | SELECT shield_id FROM ${OneTestTable.quoted} AS L1
         | INNER JOIN (SELECT * FROM ${OneTestTable.quoted}) AS R1 USING shield_id FORMAT JSON
         | """.stripMargin
    )
  }

  it should "use functions in group by method" in {
    val select = SelectQuery(Seq(shieldId))
    val query  = toSql(
      InternalQuery(
        Some(select),
        Some(TableFromQuery[OneTestTable.type](OneTestTable)),
        orderBy = Seq((lower(shieldId), ASC))
      )
    )
    query should matchSQL(s"SELECT shield_id FROM ${OneTestTable.quoted} ORDER BY lower(shield_id) ASC FORMAT JSON")
  }

  it should "use inner query as join" in {
    val select     = SelectQuery(Seq(shieldId))
    val joinSelect = SelectQuery(Seq(itemId as "shield_id"))
    val query      = toSql(
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
         | SELECT shield_id FROM ${OneTestTable.quoted} AS L1 GLOBAL
         | ANY LEFT JOIN (SELECT item_id AS shield_id FROM ${TwoTestTable.quoted}) AS R1 USING shield_id
         | FORMAT JSON""".stripMargin
    )
  }

  it should "generate CONDITIONAL cases" in {
    tokenizeColumn(switch(const(3))) shouldBe "3"
    tokenizeColumn(switch(shieldId, columnCase(col1.isEq("test"), itemId))) shouldBe
    s"CASE WHEN ${col1.name} = 'test' THEN ${itemId.name} ELSE ${shieldId.name} END"
  }

  it should "generate CONDITIONAL multiIf" in {

    // test no cases
    tokenizeColumn(multiIf(const(3))) shouldBe "3"

    // test single case
    tokenizeColumn(multiIf(shieldId, columnCase(col1.isEq("test"), itemId))) shouldBe
    s"if(${col1.name} = 'test', ${itemId.name}, ${shieldId.name})"

    // test multi cases
    tokenizeColumn(
      multiIf(shieldId, columnCase(col1.isEq("test"), itemId), columnCase(col1.isEq("test"), itemId))
    ) shouldBe
    s"multiIf(${col1.name} = 'test', ${itemId.name}, ${col1.name} = 'test', ${itemId.name}, ${shieldId.name})"
  }

  it should "use constant" in {
    tokenizeColumn(const(3).as(col2)) shouldBe s"3 AS ${col2.name}"
  }

  it should "allow to behave like little bobby tables" in {
    val col    = RawColumn("Robert'); DROP TABLE students;")
    val select = SelectQuery(Seq(col))
    val query  = toSql(InternalQuery(select = Some(select), where = Some(col)))
    query should matchSQL(s"SELECT ${col.rawSql} WHERE ${col.rawSql} FORMAT JSON")
  }

  it should "build with combinators" in {
    tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), uniq(col1))) should matchSQL(
      s"uniqIf(${col1.name}, ${col1.name} = 'test')"
    )
    tokenizeColumn(CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), uniqHLL12(col1))) should matchSQL(
      s"uniqHLL12If(${col1.name}, ${col1.name} = 'test')"
    )
    tokenizeColumn(
      CombinedAggregatedFunction(Combinator.If(col1.isEq("test")), uniqCombined(col1))
    ) should matchSQL(s"uniqCombinedIf(${col1.name}, ${col1.name} = 'test')")
    tokenizeColumn(
      CombinedAggregatedFunction(
        Combinator.If(col1.isEq("test")),
        CombinedAggregatedFunction(Combinator.If(col2.isEq(3)), uniqExact(col1))
      )
    ) should matchSQL(s"uniqExactIfIf(${col1.name}, ${col2.name} = 3, ${col1.name} = 'test')")
  }

  it should "uniq for multiple columns" in {
    tokenizeColumn(uniq(col1, col2)) should matchSQL(s"uniq(${col1.name}, ${col2.name})")
    tokenizeColumn(uniqHLL12(col1, col2)) should matchSQL(s"uniqHLL12(${col1.name}, ${col2.name})")
    tokenizeColumn(uniqExact(col1, col2)) should matchSQL(s"uniqExact(${col1.name}, ${col2.name})")
    tokenizeColumn(uniqCombined(col1, col2)) should matchSQL(s"uniqCombined(${col1.name}, ${col2.name})")
  }

  it should "use zone name for monthly" in {
    tokenizeTimeSeries(
      TimeSeries(
        timestampColumn,
        MultiInterval(
          DateTime.now(DateTimeZone.forOffsetHours(2)),
          DateTime.now(DateTimeZone.forOffsetHours(2)),
          MultiDuration(TimeUnit.Month)
        )
      )
    ) should matchSQL("toDateTime(toStartOfMonth(toDateTime(ts / 1000), 'Etc/GMT-2'), 'Etc/GMT-2')")
  }

  it should "quote them correctly" in {
    val name   = "props.key"
    val col    = RefColumn(name)
    val select = SelectQuery(Seq(col))
    val query  = toSql(InternalQuery(select = Some(select)))
    query should matchSQL(s"SELECT `$name` FORMAT JSON")
  }

  it should "do it exactly" in {
    val name     = "props.key"
    val col      = RefColumn[String](name).isEq("some  thing is  off")
    val select   = SelectQuery(Seq(col))
    val query    = toSql(InternalQuery(select = Some(select)))
    val expected = "SELECT  `props.key` = 'some  thing is  off'"
    query.slice(0, expected.length) should be(expected)
  }
}
