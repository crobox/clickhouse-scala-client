package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.misc.LogicalOperatorImprovements._

class LogicalOperatorImprovementsTest extends DslTestSpec {

  private val flag: TableColumn[Boolean]  = NativeColumn[Boolean]("flag")
  private val other: TableColumn[Boolean] = shieldId.isEq("a")

  // The magnet carries an Option[TableColumn[Boolean]], and a bare NativeColumn is a TableColumn that is not an
  // ExpressionColumn. Casting the one-sided result to ExpressionColumn threw ClassCastException on exactly that.
  it should "combine a magnet with an absent side without casting" in {
    val magnet: LogicalOpsMagnet = Option(flag)
    val absent: LogicalOpsMagnet = Option.empty[TableColumn[Boolean]]

    toSQL(new LogicalOpsMagnetImpr(magnet).and(absent).get) should matchSQL("flag")
    toSQL(new LogicalOpsMagnetImpr(absent).or(magnet).get) should matchSQL("flag")
    toSQL(new LogicalOpsMagnetImpr(magnet).xor(absent).get) should matchSQL("flag")

    new LogicalOpsMagnetImpr(absent).and(absent) shouldBe empty
  }

  it should "still combine two present sides" in {
    val left: LogicalOpsMagnet  = Option(flag)
    val right: LogicalOpsMagnet = Option(other)

    toSQL(new LogicalOpsMagnetImpr(left).and(right).get) should matchSQL("flag AND shield_id = 'a'")
    toSQL(new LogicalOpsMagnetImpr(left).or(right).get) should matchSQL("flag OR shield_id = 'a'")
    toSQL(new LogicalOpsMagnetImpr(left).xor(right).get) should matchSQL("xor(flag, shield_id = 'a')")
  }

  // A conjunction of two columns is always a LogicalFunction, so it is an ExpressionColumn and can go anywhere one
  // is asked for -- a lambda body, for instance.
  it should "type a two-column conjunction as an expression" in {
    val conjunction: ExpressionColumn[Boolean] = flag and other
    toSQL(conjunction) should matchSQL("flag AND shield_id = 'a'")
  }
}
