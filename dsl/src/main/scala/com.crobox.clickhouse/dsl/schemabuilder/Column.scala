package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.NativeColumn
import scala.reflect.runtime.universe._

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
sealed trait ColumnType

object ColumnType {

  //TODO infer the types based on the generic passed to the tablecolumn
  abstract class SimpleColumnType(value: String) extends ColumnType {

    override def toString: String = value
  }

  case object UInt8 extends SimpleColumnType("UInt8")

  val Boolean = ColumnType.UInt8

  case object UInt16 extends SimpleColumnType("UInt16")

  val Short = UInt16

  case object UInt32 extends SimpleColumnType("UInt32")

  val Int = ColumnType.UInt32

  case object UInt64 extends SimpleColumnType("UInt64")

  val Long = ColumnType.UInt64

  case object Int8 extends SimpleColumnType("Int8")

  case object Int16 extends SimpleColumnType("Int16")

  case object Int32 extends SimpleColumnType("Int32")

  case object Int64 extends SimpleColumnType("Int64")

  case object Float32 extends SimpleColumnType("Float32")

  val Float = ColumnType.Float32

  case object Float64 extends SimpleColumnType("Float64")

  val Double = ColumnType.Float64

  case object String extends SimpleColumnType("String")

  case class FixedString(length: Int) extends SimpleColumnType(s"FixedString($length)")

  val Uuid = ColumnType.String

  case object Date extends SimpleColumnType("Date")

  case object DateTime extends SimpleColumnType("DateTime")

  case class Array(columnType: ColumnType) extends ColumnType {
    require(!columnType.isInstanceOf[Nested] && !columnType.isInstanceOf[Array],
            "Only simple types are allowed in Array")

    override def toString: String = s"Array($columnType)"
  }

  case class Nested(columns: NativeColumn[_]*) extends ColumnType {
    require(!columns.exists(c => c.clickhouseType.isInstanceOf[Nested]), "Only a single nesting level is supported.")

    override def toString: String = s"Nested(${columns.map(_.query()).mkString(", ")})"
  }

//  TODO modifi this to accept and expression column
  case class AggregateFunction(function: String, columnType: ColumnType)
      extends SimpleColumnType(s"AggregateFunction($function, ${columnType.toString})")

}

sealed trait DefaultValue

object DefaultValue {

  case object NoDefault extends DefaultValue {

    override def toString: String = ""
  }

  case class Default(value: String) extends DefaultValue {

    override def toString: String = " DEFAULT " + value
  }

  case class Materialized(value: String) extends DefaultValue {

    override def toString: String = " MATERIALIZED " + value
  }

}
