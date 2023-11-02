package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

import java.util.UUID

class StringFunctionsIT extends DslITSpec {
  private val columnString = "oneem,twoem,threeem"
  override val table2Entries: Seq[Table2Entry] =
    Seq(Table2Entry(UUID.randomUUID(), columnString, randomInt, randomString, None))

  it should "split by character" in {
    val resultRows =
      queryExecutor
        .execute[StringResult](select(arrayJoin(splitByChar(",", col1)) as "result") from TwoTestTable)
        .futureValue
        .rows
    resultRows.length shouldBe 3
    resultRows.map(_.result) should contain theSameElementsAs Seq("oneem", "twoem", "threeem")
  }

  it should "split by string" in {
    val resultRows =
      queryExecutor
        .execute[StringResult](select(arrayJoin(splitByString("em,", col1)) as "result") from TwoTestTable)
        .futureValue
        .rows
    resultRows.length shouldBe 3
    resultRows.map(_.result) should contain theSameElementsAs Seq("one", "two", "threeem")
  }

  it should "concatenate string back" in {
    val resultRows =
      queryExecutor
        .execute[StringResult](select(arrayStringConcat(splitByChar(",", col1), ",") as "result") from TwoTestTable)
        .futureValue
        .rows
    resultRows.length shouldBe 1
    resultRows.map(_.result).head shouldBe columnString
  }
}
