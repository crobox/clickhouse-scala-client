package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl.typed.TypedQuery
import com.crobox.clickhouse.internal.QuerySettings
import org.joda.time.DateTime

import java.util.UUID

class TypedQueryIT extends DslITSpec {

  private val idA = UUID.randomUUID()
  private val idB = UUID.randomUUID()

  override val table1Entries: Seq[Table1Entry] = Seq(
    Table1Entry(idA, DateTime.now(), Seq(1, 2, 3)),
    Table1Entry(idB, DateTime.now(), Seq(4, 5))
  )

  override val table2Entries: Seq[Table2Entry] = Seq(
    Table2Entry(idA, "one", 11, "three", None),
    Table2Entry(idB, "two", 22, "three", None)
  )

  it should "decode a multi-column result into a tuple, with no hand-written reader" in {
    val query: TypedQuery[(String, Int)] = typed.select(itemId, col2) from TwoTestTable orderBy col2
    queryExecutor.executeTyped(query).futureValue.rows shouldBe Seq((idA.toString, 11), (idB.toString, 22))
  }

  it should "decode a single column as the value itself, not a Tuple1" in {
    val query: TypedQuery[Int] = typed.select(col2) from TwoTestTable orderBy col2
    queryExecutor.executeTyped(query).futureValue.rows shouldBe Seq(11, 22)
  }

  it should "decode a UInt64 aggregate into Long" in {
    queryExecutor.executeTyped(typed.select(count()) from TwoTestTable).futureValue.rows shouldBe Seq(2L)
  }

  it should "decode an array column into a Seq" in {
    val query: TypedQuery[Seq[Int]] = typed.select(numbers) from OneTestTable orderBy numbers
    queryExecutor.executeTyped(query).futureValue.rows should contain theSameElementsAs Seq(Seq(1, 2, 3), Seq(4, 5))
  }

  it should "decode a 64-bit result whichever way the server spells it" in {
    // The whole point of decoding by declared type. output_format_json_quote_64bit_integers defaulted to 1 through 25.3
    // and to 0 from 25.8; reference.conf currently pins it to 1 so hand-written readers keep working. This path does not
    // care, so that pin can go once the untyped API does.
    val query = typed.select(count()) from TwoTestTable
    for (quoted <- Seq("0", "1")) {
      implicit val settings: QuerySettings =
        QuerySettings(settings = Map("output_format_json_quote_64bit_integers" -> quoted))
      withClue(s"with output_format_json_quote_64bit_integers=$quoted: ") {
        queryExecutor.executeTyped(query).futureValue.rows shouldBe Seq(2L)
      }
    }
  }

  it should "group by a column outside the select list without widening the row" in {
    // The untyped groupBy appends the grouping column to the select list; if that leaked through, this would come back
    // as (Long, String) against an arity-1 decoder and fail to parse.
    val query: TypedQuery[Long] = typed.select(count()) from TwoTestTable groupBy col3
    queryExecutor.executeTyped(query).futureValue.rows shouldBe Seq(2L)
  }

  it should "order by a column outside the select list without widening the row" in {
    val query: TypedQuery[String] = typed.select(itemId) from TwoTestTable orderBy col2
    queryExecutor.executeTyped(query).futureValue.rows shouldBe Seq(idA.toString, idB.toString)
  }

  it should "carry the declared ClickHouse types through as meta" in {
    val meta = queryExecutor.executeTyped(typed.select(itemId, col2) from TwoTestTable).futureValue.meta
    meta.map(_.columnTypes.map(c => c.name -> c.columnType)) shouldBe
    Some(Seq("item_id" -> "String", "column_2" -> "UInt32"))
  }

  it should "compose with the untyped combinators it does not forward" in {
    val query = (typed.select(col2) from TwoTestTable).transform(_.where(col2 > 11))
    queryExecutor.executeTyped(query).futureValue.rows shouldBe Seq(22)
  }

  it should "leave the untyped path on FORMAT JSON, so existing readers keep working" in {
    // This layer is additive. The compact format is chosen by executeTyped, not by the tokenizer's default, so nothing
    // about the untyped API or anyone's hand-written JsonReader changes.
    import com.crobox.clickhouse.DslITSpec.{intResultFormat, IntResult}

    val untyped = select(col2 as "result") from TwoTestTable orderBy col2
    val sql     = toSql(untyped.internalQuery)
    sql should endWith("FORMAT JSON")
    sql should not include "Compact"

    queryExecutor.execute[IntResult](untyped).futureValue.rows.map(_.result) shouldBe Seq(11, 22)
  }
}
