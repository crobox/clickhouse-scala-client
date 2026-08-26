package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class DecimalAndIPColumnTypeTest extends DslTestSpec {

  "Decimal" should "render its precision and scale" in {
    ColumnType.Decimal(10, 2).toString shouldBe "Decimal(10, 2)"
    ColumnType.Decimal(76, 76).toString shouldBe "Decimal(76, 76)"
  }

  it should "refuse a precision outside 1 to 76" in {
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal(0, 0)
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal(77, 2)
  }

  it should "refuse a scale above the precision, but allow one equal to it" in {
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal(10, 11)
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal(10, -1)
    ColumnType.Decimal(10, 10).scale shouldBe 10
  }

  "The sized Decimals" should "render their scale and know their implied precision" in {
    ColumnType.Decimal32(4).toString shouldBe "Decimal32(4)"
    ColumnType.Decimal256(2).toString shouldBe "Decimal256(2)"
    Seq(
      ColumnType.Decimal32(0)  -> 9,
      ColumnType.Decimal64(0)  -> 18,
      ColumnType.Decimal128(0) -> 38,
      ColumnType.Decimal256(0) -> 76
    ).foreach { case (columnType, precision) => columnType.precision shouldBe precision }
  }

  it should "refuse a scale above the implied precision" in {
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal32(10)
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal64(19)
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal128(39)
    an[IllegalArgumentException] should be thrownBy ColumnType.Decimal256(77)
  }

  "IPv4 and IPv6" should "render their names" in {
    ColumnType.IPv4.toString shouldBe "IPv4"
    ColumnType.IPv6.toString shouldBe "IPv6"
  }

  "A column declaration" should "carry the new types" in {
    NativeColumn[BigDecimal]("amount", ColumnType.Decimal(10, 2)).query shouldBe "amount Decimal(10, 2)"
    NativeColumn[String]("addr", ColumnType.IPv4).query shouldBe "addr IPv4"
    NativeColumn[BigDecimal]("rate", ColumnType.Nullable(ColumnType.Decimal64(4))).query shouldBe
    "rate Nullable(Decimal64(4))"
  }

  "cast" should "type a Decimal target as BigDecimal and an IP target as String" in {
    toSQL(select(cast(col2, ColumnType.Decimal(10, 2))), false) should matchSQL(
      "SELECT cast(column_2 AS Decimal(10, 2))"
    )
    // Typing, not just rendering: these only compile if the CastOutBind resolves to the right output type.
    val amount: TableColumn[BigDecimal] = cast(col2, ColumnType.Decimal(10, 2))
    val scaled: TableColumn[BigDecimal] = cast(col2, ColumnType.Decimal64(4))
    val address: TableColumn[String]    = cast(col3, ColumnType.IPv4)
    Seq(amount, scaled, address) should have size 3
  }
}
