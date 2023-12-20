package com.crobox.clickhouse.misc

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl.misc.DSLImprovements.ColumnsImprovements
import com.crobox.clickhouse.dsl.{Column, NativeColumn}
import org.joda.time.DateTime

import java.util.UUID

class DSLImprovementsTest extends DslTestSpec {

  val intCol: NativeColumn[Int]                 = NativeColumn[Int]("int")
  val doubleCol: NativeColumn[Double]           = NativeColumn[Double]("double")
  val stringCol: NativeColumn[String]           = NativeColumn[String]("string")
  val boolCol: NativeColumn[Boolean]            = NativeColumn[Boolean]("bool")
  val dateTimeCol: NativeColumn[DateTime]       = NativeColumn[DateTime]("date_time")
  val intArrayCol: NativeColumn[Seq[Int]]       = NativeColumn[Seq[Int]]("int_array")
  val doubleArrayCol: NativeColumn[Seq[Double]] = NativeColumn[Seq[Double]]("double_array")
  val stringArrayCol: NativeColumn[Seq[String]] = NativeColumn[Seq[String]]("string_array")
  val longArrayCol: NativeColumn[Seq[Long]]     = NativeColumn[Seq[Long]]("long_array")
  val uuidCol: NativeColumn[UUID]               = NativeColumn[UUID]("uuid")

  it should "not add column when exist" in {
    val columns = Seq(intCol, stringCol, doubleArrayCol, stringArrayCol)
    columns.addColumns(intCol) should be(columns)
    columns.addColumns(stringCol) should be(columns)
    columns.addColumns(doubleArrayCol) should be(columns)
  }

  it should "add column when not exist" in {
    val columns: Seq[Column] = Seq()
    columns.addColumns(intCol) should contain allElementsOf Iterable(intCol)
    columns.addColumns(stringCol) should contain allElementsOf Iterable(stringCol)
    columns.addColumns(doubleArrayCol) should contain allElementsOf Iterable(doubleArrayCol)
  }

  it should "not add alias column when column" in {
    val aliased = intCol as "alias"

    // set to true
    var cols = Seq(intCol).addColumn(aliased)
    cols.size should be(2)
    cols should contain allElementsOf Iterable(intCol, aliased)

    cols = Seq(intCol).addColumn(intCol)
    cols.size should be(1)
    cols should contain allElementsOf Iterable(intCol)

    cols = Seq(aliased).addColumn(intCol)
    cols.size should be(2)
    cols should contain allElementsOf Iterable(aliased, intCol)

    cols = Seq(aliased).addColumn("alias", intCol)
    cols.size should be(1)
    cols should contain allElementsOf Iterable(aliased)

    cols = Seq(aliased).addColumn(aliased)
    cols.size should be(1)
    cols should contain allElementsOf Iterable(aliased)
  }

  it should "replace column" in {
    val aliased = intCol as "alias"

    // set to true
    var cols = Seq(intCol).replaceColumn(aliased)
    cols.size should be(2)
    cols should be(Seq(intCol, aliased))
    cols = Seq(intCol).replaceColumn("alias", aliased)
    cols.size should be(2)
    cols should be(Seq(intCol, aliased))

    // switch order
    cols = Seq(aliased).replaceColumn(intCol)
    cols.size should be(2)
    cols should be(Seq(aliased, intCol))
    cols = Seq(aliased).replaceColumn("alias", intCol)
    cols.size should be(1)
    cols should be(Seq(intCol))

    //preserve index at start
    cols = Seq(aliased, stringCol, stringArrayCol, intArrayCol).replaceColumn("alias", intCol)
    cols.size should be(4)
    cols should be(Seq(intCol, stringCol, stringArrayCol, intArrayCol))

    // preserve index somewhere in the middle
    cols = Seq(stringCol, stringArrayCol, aliased, intArrayCol).replaceColumn("alias", intCol)
    cols.size should be(4)
    cols should be(Seq(stringCol, stringArrayCol, intCol, intArrayCol))

    // preserve index at end
    cols = Seq(stringCol, stringArrayCol, intArrayCol, aliased).replaceColumn("alias", intCol)
    cols.size should be(4)
    cols should be(Seq(stringCol, stringArrayCol, intArrayCol, intCol))

  }
}
