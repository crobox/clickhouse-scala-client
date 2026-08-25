package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.NativeColumn

/**
 * @author
 *   Sjoerd Mulder
 * @since 30-12-16
 */
sealed trait ColumnType

object ColumnType {

  // TODO infer the types based on the generic passed to the tablecolumn
  abstract class SimpleColumnType(value: String) extends ColumnType {

    override def toString: String = value
  }

  case object UInt8 extends SimpleColumnType("UInt8")

  val Boolean: ColumnType = UInt8

  case object UInt16 extends SimpleColumnType("UInt16")

  case object UInt32 extends SimpleColumnType("UInt32")

  case object UInt64 extends SimpleColumnType("UInt64")

  case object Int8 extends SimpleColumnType("Int8")

  case object Int16 extends SimpleColumnType("Int16")

  val Short: ColumnType = Int16

  case object Int32 extends SimpleColumnType("Int32")

  val Int: ColumnType = Int32

  case object Int64 extends SimpleColumnType("Int64")

  val Long: ColumnType = Int64

  case object Float32 extends SimpleColumnType("Float32")

  val Float: ColumnType = Float32

  case object Float64 extends SimpleColumnType("Float64")

  val Double: ColumnType = Float64

  case object String extends SimpleColumnType("String")

  case class FixedString(length: Int) extends SimpleColumnType(s"FixedString($length)")

  case object UUID extends SimpleColumnType("UUID")

  case object Date extends SimpleColumnType("Date")

  case object DateTime extends SimpleColumnType("DateTime")

  /**
   * `DateTime64(precision[, timezone])`, where precision is the number of fractional-second digits.
   *
   * The reader keeps all nine: ColumnDecoder parses the fractional part into a ZonedDateTime's nanoseconds.
   */
  case class DateTime64(precision: Int, timezone: Option[String] = None) extends ColumnType {
    require(precision >= 0 && precision <= 9, s"DateTime64 precision must be between 0 and 9, got $precision")

    override def toString: String =
      timezone.map(zone => s"DateTime64($precision, '$zone')").getOrElse(s"DateTime64($precision)")
  }

  case class Array(columnType: ColumnType) extends ColumnType {
    require(
      !columnType.isInstanceOf[Nested] && !columnType.isInstanceOf[Array],
      "Only simple types are allowed in Array"
    )

    override def toString: String = s"Array($columnType)"
  }

  case class Nested(columns: NativeColumn[_]*) extends ColumnType {
    require(!columns.exists(c => c.clickhouseType.isInstanceOf[Nested]), "Only a single nesting level is supported.")

    override def toString: String = s"Nested(${columns.map(_.query).mkString(", ")})"
  }

//  TODO modify this to accept and expression column
  case class AggregateFunctionColumn(function: String, columnType: ColumnType, nextTypes: ColumnType*)
      extends SimpleColumnType(
        s"AggregateFunction($function, ${(columnType +: nextTypes).map(_.toString).mkString(", ")})"
      )

  case class LowCardinality(columnType: ColumnType) extends ColumnType {
    override def toString: String = s"LowCardinality(${columnType.toString})"
  }

  case class Nullable(columnType: ColumnType) extends ColumnType {
    override def toString: String = s"Nullable(${columnType.toString})"
  }

  /** `Variant(A, B, ...)`: one value, one of several types, discriminated per row. */
  case class Variant(columnType: ColumnType, nextTypes: ColumnType*) extends ColumnType {
    require(nextTypes.nonEmpty, "Variant needs at least two alternatives")

    override def toString: String = s"Variant(${(columnType +: nextTypes).mkString(", ")})"
  }

  /** `Dynamic`: any type, decided per row rather than declared. */
  case object Dynamic extends SimpleColumnType("Dynamic")

  /** The native `JSON` type, as opposed to the `visitParam*` functions that read JSON out of a String. */
  case object JSON extends SimpleColumnType("JSON")

  /**
   * `SimpleAggregateFunction(f, types...)`.
   *
   * Unlike [[AggregateFunctionColumn]] this stores the aggregated value itself rather than an intermediate state, so it
   * is read like an ordinary column and needs no `-Merge`.
   */
  case class SimpleAggregateFunction(function: String, columnType: ColumnType, nextTypes: ColumnType*)
      extends SimpleColumnType(
        s"SimpleAggregateFunction($function, ${(columnType +: nextTypes).map(_.toString).mkString(", ")})"
      )
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
