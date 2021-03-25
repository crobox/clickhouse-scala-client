package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl.misc.DSLImprovements.ColumnsImprv
import com.crobox.clickhouse.dsl.{Column, NativeColumn}

class DSLImprovementsTest extends ClickhouseClientSpec {
  val c1: NativeColumn[String]      = NativeColumn[String]("c1")
  val c2: NativeColumn[String]      = NativeColumn[String]("c2")
  val c3: NativeColumn[Int]         = NativeColumn[Int]("c3")
  val c4: NativeColumn[Boolean]     = NativeColumn[Boolean]("c4")
  val c5: NativeColumn[Double]      = NativeColumn[Double]("c5")
  val columns: Seq[NativeColumn[_]] = Seq(c1, c2, c3, c4)

  it should "add column" in {
    // try to add existing column
    columns.addColumn(c3) should be(Iterable(c1, c2, c3, c4))
    columns.addColumn(c5) should be(Iterable(c1, c2, c3, c4, c5))

    // add with empty list
    Seq.empty[Column].addColumn(c1) should be(Iterable(c1))
  }

  it should "remove column" in {
    columns.removeColumn(c5) should be(Iterable(c1, c2, c3, c4))
    columns.removeColumn(c3) should be(Iterable(c1, c2, c4))

    // remove with empty list
    Seq.empty[Column].removeColumn(c3) should be(Iterable.empty)
  }
}
