package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

/**
 * The declarations round-tripped through a real table: that the server accepts them, reports them back as expected, and
 * that the existing decoders read the values without any new marshalling.
 */
class DecimalAndIPColumnTypeIT extends DslITSpec {

  private object TypedTable extends Table {
    override val database                = DecimalAndIPColumnTypeIT.this.database
    override val name                    = "decimalAndIpTable"
    val amount: NativeColumn[BigDecimal] = NativeColumn("amount", ColumnType.Decimal(10, 2))
    val rate: NativeColumn[BigDecimal]   = NativeColumn("rate", ColumnType.Decimal32(4))
    val big: NativeColumn[BigDecimal]    = NativeColumn("big", ColumnType.Decimal256(10))
    val v4: NativeColumn[String]         = NativeColumn("v4", ColumnType.IPv4)
    val v6: NativeColumn[String]         = NativeColumn("v6", ColumnType.IPv6)
    override val columns                 = List(amount, rate, big, v4, v6)
  }

  private case class Entry(amount: BigDecimal, rate: BigDecimal, big: BigDecimal, v4: String, v6: String)
  private implicit val entryFormat: RootJsonFormat[Entry] = jsonFormat5(Entry.apply)

  private lazy val row = {
    clickClient.execute(CreateTable(TypedTable, Engine.Memory, ifNotExists = true).query).futureValue
    clickClient.execute(s"TRUNCATE TABLE ${TypedTable.quoted}").futureValue
    queryExecutor
      .insert(
        TypedTable,
        Seq(Entry(BigDecimal("1.25"), BigDecimal("2.5"), BigDecimal("3.125"), "10.0.0.1", "2001:db8::1"))
      )
      .futureValue
    queryExecutor.executeRows(select(com.crobox.clickhouse.dsl.all) from TypedTable).futureValue.rows.head
  }

  it should "accept every declaration and report the type back" in {
    // Decimal32(4) is Decimal(9, 4) and Decimal256(10) is Decimal(76, 10): the sized spelling is an alias, so the
    // server reports the canonical form rather than the one that was declared.
    row.header.declaredTypeOf("amount") shouldBe Option("Decimal(10, 2)")
    row.header.declaredTypeOf("rate") shouldBe Option("Decimal(9, 4)")
    row.header.declaredTypeOf("big") shouldBe Option("Decimal(76, 10)")
    row.header.declaredTypeOf("v4") shouldBe Option("IPv4")
    row.header.declaredTypeOf("v6") shouldBe Option("IPv6")
  }

  it should "read the decimals back through the existing BigDecimal decoder" in {
    row.requiredByName[BigDecimal]("amount") shouldBe BigDecimal("1.25")
    row.requiredByName[BigDecimal]("rate") shouldBe BigDecimal("2.5")
    row.requiredByName[BigDecimal]("big") shouldBe BigDecimal("3.125")
  }

  it should "read the addresses back as their textual form" in {
    row.requiredByName[String]("v4") shouldBe "10.0.0.1"
    row.requiredByName[String]("v6") shouldBe "2001:db8::1"
  }

  it should "cast to a Decimal and to an IP" in {
    r(toTypeName(cast(const(1), ColumnType.Decimal(10, 2)))) shouldBe "Decimal(10, 2)"
    r(toTypeName(cast(const("10.0.0.1"), ColumnType.IPv4))) shouldBe "IPv4"
  }
}
