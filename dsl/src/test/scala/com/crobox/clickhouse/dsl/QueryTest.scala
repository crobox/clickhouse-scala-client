package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.dsl.column._

import com.crobox.clickhouse.dsl.JoinQuery.AnyInnerJoin
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import org.joda.time.{DateTime, LocalDate}

import scala.util.{Failure, Success}

class QueryTest extends ClickhouseClientSpec with TestSchema {
  val clickhouseTokenizer = new ClickhouseTokenizerModule {}
  override val database: String = "query_test"

  "querying using the typed query" should "perform simple select" in {
    val query = select(shieldId) from OneTestTable
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT shield_id FROM $database.captainAmerica FORMAT JSON"
    )
  }

  it should "generate for join between tables" in {
    val query = select(col1, shieldId).from(OneTestTable).join(AnyInnerJoin, TwoTestTable) using shieldId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT column_1, shield_id FROM $database.captainAmerica ANY INNER JOIN (SELECT * FROM $database.twoTestTable) USING shield_id FORMAT JSON"
    )
  }

  it should "generate inner join" in {
    val expectedUUID                     = UUID.randomUUID()
    val innerQuery: OperationalQuery     = select(shieldId as itemId) from OneTestTable where shieldId.isEq(expectedUUID)
    val joinInnerQuery: OperationalQuery = select(itemId) from TwoTestTable where (col3 isEq "wompalama")
    val query                            = select(col1, shieldId) from innerQuery join (AnyInnerJoin, joinInnerQuery) using itemId
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT column_1, shield_id FROM (SELECT shield_id AS item_id FROM $database.captainAmerica WHERE shield_id = '$expectedUUID' ) ANY INNER JOIN (SELECT item_id FROM $database.twoTestTable WHERE column_3 = 'wompalama' ) USING item_id FORMAT JSON"
    )
  }

  it should "escape from evil" in {
    val query = select(shieldId) from OneTestTable where col3.isEq("use ' evil")
    clickhouseTokenizer.toSql(query.internalQuery) should be(
      s"SELECT shield_id FROM $database.captainAmerica WHERE column_3 = 'use \\' evil' FORMAT JSON"
    )
  }

  it should "overrule with left preference" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from OneTestTable where col2 >= 2
    val composed = query :+> query2
    clickhouseTokenizer.toSql(composed.internalQuery) should be (
      s"SELECT shield_id FROM $database.captainAmerica WHERE column_2 >= 2 FORMAT JSON"
    )
  }

  it should "compose indexOf and arrayElement" in {

    def lookupNestedValue(column: NativeColumn[_], elm: String): ExpressionColumn[String] = column.clickhouseType match {
      case ColumnType.Nested(k, v) =>
        val keyColumn = ref[Seq[String]](column.name +"."+ k.name)
        val valueColumn = ref[Seq[String]](column.name +"."+ v.name)
        arrayElement(valueColumn, indexOf(keyColumn, elm))
      case _ =>
        throw new IllegalArgumentException(s"ColumnType ${column.clickhouseType} is unsupported for nested lookup")
    }

    val nested = NativeColumn("props", ColumnType.Nested(NativeColumn("key"), NativeColumn("value")))
    clickhouseTokenizer.toSql(
      select(lookupNestedValue(nested, "cate'gory")).internalQuery
    ) should be (

      "SELECT `props.value`[indexOf(`props.key`,'cate\\'gory')] FORMAT JSON"
    )

  }

  it should "overrule with right preference" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from OneTestTable where col2 >= 2
    val composed = query <+: query2
    clickhouseTokenizer.toSql(composed.internalQuery) should be (
      s"SELECT item_id FROM $database.captainAmerica WHERE column_2 >= 2 FORMAT JSON"
    )
  }

  it should "fail on try override of conflicting queries" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from OneTestTable where col2 >= 2
    val composed = query + query2
    composed should matchPattern {
      case Failure(_:IllegalArgumentException) =>
    }
  }

  it should "parse datefunction" in {
    val query = select(toYear(NativeColumn[DateTime]("dateTime"))) from OneTestTable
    val s = clickhouseTokenizer.toSql(query.internalQuery)

    s.nonEmpty shouldBe true
  }

  it should "parse column function in filter" in {

    val query = select(minus(NativeColumn[LocalDate]("date"), NativeColumn[Double]("double"))) from OneTestTable where(sum(col2) > 0)
    val s = clickhouseTokenizer.toSql(query.internalQuery)

    s shouldBe s"SELECT date - double FROM $database.captainAmerica WHERE sum(column_2) > 0 FORMAT JSON"
  }

  it should "parse const as column for magnets" in {
    val query = select(col2 - 1, intDiv(2,3)) from OneTestTable
    val s = clickhouseTokenizer.toSql(query.internalQuery)
    s shouldBe s"SELECT column_2 - 1, intDiv(2, 3) FROM $database.captainAmerica FORMAT JSON"
  }


  it should "succeed on safe override of non-conflicting multi part queries" in {
    val query = select(shieldId)
    val query2 = from(OneTestTable)
    val query3 = where(col2 >= 4)

    val composed = query + query2
    val composed2 = composed + query3

    composed should matchPattern {
      case t: Success[_] =>
    }

    clickhouseTokenizer.toSql(composed.get.internalQuery) should be (
      s"SELECT shield_id FROM $database.captainAmerica FORMAT JSON"
    )

    composed2 should matchPattern {
      case t: Success[_] =>
    }
    clickhouseTokenizer.toSql(composed2.get.internalQuery) should be (
      s"SELECT shield_id FROM $database.captainAmerica WHERE column_2 >= 4 FORMAT JSON"
    )
  }

  it should "throw an exception if the union doesn't have the same number of columns" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(shieldId, itemId) from OneTestTable

    an[IllegalArgumentException] should be thrownBy  {
      query.unionAll(query2)
    }
  }

  it should "perform the union of multiple tables" in {
    val query = select(shieldId) from OneTestTable
    val query2 = select(itemId) from TwoTestTable
    val query3 = select(itemId) from ThreeTestTable
    val union = query.unionAll(query2).unionAll(query3)
    val generatedSql = clickhouseTokenizer.toSql(union.internalQuery)

    generatedSql should be (
      s"SELECT shield_id FROM $database.captainAmerica UNION ALL SELECT item_id FROM $database.twoTestTable UNION ALL SELECT item_id FROM $database.threeTestTable FORMAT JSON"
    )
  }

  it should "select from an union of two tables" in {
    val query2 = select(itemId) from TwoTestTable
    val query3 = select(itemId) from ThreeTestTable
    val query = select(itemId) from query2.unionAll(query3)

    val generatedSql = clickhouseTokenizer.toSql(query.internalQuery)

    generatedSql should be (
      s"SELECT item_id FROM (SELECT item_id FROM $database.twoTestTable UNION ALL SELECT item_id FROM $database.threeTestTable) FORMAT JSON"
    )
  }
}
