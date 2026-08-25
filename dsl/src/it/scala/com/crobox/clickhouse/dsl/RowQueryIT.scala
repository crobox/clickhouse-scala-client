package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl.execution.ColumnLookupException
import com.crobox.clickhouse.internal.QuerySettings
import java.time.{Instant, LocalDate, ZoneId, ZoneOffset, ZonedDateTime}

import java.util.UUID

/**
 * The shapes here are taken from how the DSL is used against real schemas rather than invented: select lists built from
 * a runtime `Seq[Column]`, `select(all)`, columns pulled into the projection by `groupBy`, and a reader that consumes a
 * set of dimensions only known at request time.
 */
class RowQueryIT extends DslITSpec {

  private val idA = UUID.randomUUID()
  private val idB = UUID.randomUUID()

  override val table1Entries: Seq[Table1Entry] = Seq(
    Table1Entry(idA, ZonedDateTime.now(), Seq(1, 2, 3)),
    Table1Entry(idB, ZonedDateTime.now(), Seq(4, 5))
  )

  override val table2Entries: Seq[Table2Entry] = Seq(
    Table2Entry(idA, "one", 11, "three", None),
    Table2Entry(idB, "two", 22, "three", None)
  )

  it should "read columns off a result with no hand-written reader" in {
    val query = select(itemId, col2) from TwoTestTable orderBy col2
    val rows  = queryExecutor.executeRows(query).futureValue.rows
    rows.map(r => (r.get(itemId), r.get(col2))) shouldBe Seq(
      (Some(idA.toString), Some(11)),
      (Some(idB.toString), Some(22))
    )
  }

  it should "read a column the select list only names at runtime" in {
    // select(projections: _*) -- the arity and the columns come from a Seq, so nothing about this is known at compile time.
    val projections: Seq[Column] = Seq(itemId, col2, col3)
    val query                    = select(projections: _*) from TwoTestTable orderBy col2
    val rows                     = queryExecutor.executeRows(query).futureValue.rows

    rows.map(_.get(col3)) shouldBe Seq(Some("three"), Some("three"))
    rows.head.names should contain allOf ("item_id", "column_2", "column_3")
  }

  it should "read a column out of a select-star" in {
    val rows = queryExecutor.executeRows(select(All()) from TwoTestTable orderBy col2).futureValue.rows
    rows.map(_.get(col2)) shouldBe Seq(Some(11), Some(22))
  }

  it should "read a column that groupBy pulled into the select list" in {
    // The idiom this design exists for: col3 is not in the select list, groupBy merges it in, and the reader picks it
    // up by name. A positional row type cannot express this -- the row is wider than the select list as written.
    val query = select(count() as "total") from TwoTestTable groupBy col3
    val rows  = queryExecutor.executeRows(query).futureValue.rows

    rows.map(r => (r.get(col3), r.getByName[Long]("total"))) shouldBe Seq((Some("three"), Some(2L)))
  }

  it should "read an aggregate back by its alias" in {
    val total = NativeColumn[Long]("total")
    val query = select(col3, count() as total) from TwoTestTable groupBy col3
    queryExecutor.executeRows(query).futureValue.rows.map(_.get(total)) shouldBe Seq(Some(2L))
  }

  it should "let a reader sweep the columns it was not told about" in {
    // The dynamic-dimensions shape: pull out the known column, treat whatever else came back as the dimensions.
    val query      = select(itemId, col2, col3) from TwoTestTable orderBy col2
    val row        = queryExecutor.executeRows(query).futureValue.rows.head
    val dimensions = row.fields.filterNot { case (name, _) => name == itemId.name }

    dimensions.keySet shouldBe Set("column_2", "column_3")
  }

  it should "decode a 64-bit result whichever way the server spells it" in {
    // output_format_json_quote_64bit_integers defaulted to 1 through 25.3 and to 0 from 25.8; reference.conf pins it to
    // 1 so hand-written readers keep working. This path does not care, so that pin can go once the untyped API does.
    val query = select(count() as "total") from TwoTestTable
    for (quoted <- Seq("0", "1")) {
      implicit val settings: QuerySettings =
        QuerySettings(settings = Map("output_format_json_quote_64bit_integers" -> quoted))
      withClue(s"with output_format_json_quote_64bit_integers=$quoted: ") {
        queryExecutor.executeRows(query).futureValue.rows.head.getByName[Long]("total") shouldBe Some(2L)
      }
    }
  }

  it should "carry the declared ClickHouse types through as meta" in {
    val meta = queryExecutor.executeRows(select(itemId, col2) from TwoTestTable).futureValue.meta
    meta.map(_.columnTypes.map(c => c.name -> c.columnType)) shouldBe
    Some(Seq("item_id" -> "String", "column_2" -> "UInt32"))
  }

  it should "name what came back when a column is not in the result" in {
    val row     = queryExecutor.executeRows(select(itemId) from TwoTestTable).futureValue.rows.head
    val failure = intercept[ColumnLookupException](row(col2))
    failure.getMessage should include("column_2")
    failure.getMessage should include("item_id")
  }

  it should "leave the untyped path on FORMAT JSON, so existing readers keep working" in {
    // This layer is additive. The compact format is chosen by executeRows, not by the tokenizer's default, so nothing
    // about the untyped API or anyone's hand-written JsonReader changes.
    import com.crobox.clickhouse.DslITSpec.{intResultFormat, IntResult}

    val untyped = select(col2 as "result") from TwoTestTable orderBy col2
    val sql     = toSql(untyped.internalQuery)
    sql should endWith("FORMAT JSON")
    sql should not include "Compact"

    queryExecutor.execute[IntResult](untyped).futureValue.rows.map(_.result) shouldBe Seq(11, 22)
  }
}
