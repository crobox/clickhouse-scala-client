package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl.misc.LogicalOperatorImprovements.ExpressionColumnImpr
import com.crobox.clickhouse.dsl.select
import com.crobox.clickhouse.dsl._
import scala.language.implicitConversions

import java.util.UUID

class INFunctionsIT extends DslITSpec {

  override val table2Entries: Seq[Table2Entry] =
    Seq(
      Table2Entry(UUID.randomUUID(), "a", randomInt, randomString, Option("a")),
      Table2Entry(UUID.randomUUID(), "b", randomInt, randomString, Option("b")),
      Table2Entry(UUID.randomUUID(), "c", randomInt, randomString, Option("c"))
    )
  override val table3Entries: Seq[Table3Entry] =
    Seq(
      Table3Entry(UUID.randomUUID(), 2, Option("a"), "a", "a"),
      Table3Entry(UUID.randomUUID(), 2, Option("a"), "b", "b"),
      Table3Entry(UUID.randomUUID(), 2, Option("b"), "c", "c")
    )

  // TODO: Temporary Scala 3 Workaround. Somehow in CI/CD version of Scala 3, columns are not converted to respectible
  // Magnet instances, so that `in` method could be used. Need to explicitly specify type annotation for conversion to
  // work. This may require more deep work on this in the future
  private val constCol4: ConstOrColMagnet[String] = col4
  private val constCol2: ConstOrColMagnet[Int] = col2

  it should "use tableAlias for IN, single table" in {
    execute(
      select(col4).from(TwoTestTable).where(constCol4.in(select(col4).from(ThreeTestTable)))
    ).futureValue should be("a\nb")
  }

  it should "use tableAlias for IN multiple tables" in {
    assumeMinimalClickhouseVersion(21)

    // check if syntax is correct
    execute(
      select(col4)
        .from(TwoTestTable)
        .where(
          constCol4.in(select(col4).from(ThreeTestTable)) and
            constCol2.in(select(col2).from(TwoTestTable)) and
            constCol2.in(select(col4).from(ThreeTestTable))
        )
    ).futureValue should be("")
  }
}
