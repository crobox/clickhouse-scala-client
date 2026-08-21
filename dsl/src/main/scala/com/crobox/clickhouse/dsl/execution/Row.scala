package com.crobox.clickhouse.dsl.execution

import com.crobox.clickhouse.dsl.TableColumn
import com.crobox.clickhouse.dsl.marshalling.{ColumnDecoder, ColumnDecodingException}
import spray.json._

/**
 * The column names and declared ClickHouse types of a result, shared by every row in it.
 *
 * Held once per result rather than per row: `JSONCompactEachRowWithNamesAndTypes` states the names and types in two
 * header lines and then sends bare arrays, so a result of a million rows carries one copy of this rather than repeating
 * every field name a million times as `FORMAT JSON` does.
 */
final class ResultHeader private[execution] (val names: Vector[String], val types: Vector[String]) {

  private val positions: Map[String, Int] = names.zipWithIndex.toMap

  /**
   * Names that appear more than once. A select list can produce these -- `sum(x)` and `avg(x)` are both named after `x`
   * unless aliased -- and picking either one silently would be worse than refusing.
   */
  private val ambiguous: Set[String] = names.diff(names.distinct).toSet

  def indexOf(name: String): Option[Int] =
    if (ambiguous(name))
      throw ColumnLookupException(
        s"Column '$name' appears ${names.count(_ == name)} times in the result, so reading it by name is ambiguous. " +
          "Alias the columns to tell them apart."
      )
    else positions.get(name)

  def declaredTypeOf(name: String): Option[String] = indexOf(name).map(types)

  def size: Int = names.size

  override def toString: String = names.zip(types).map { case (n, t) => s"$n $t" }.mkString("[", ", ", "]")
}

/**
 * One row of a result, read by column rather than by position.
 *
 * The select list does not have to be known at compile time, which is the point: a query built from a runtime
 * `Seq[Column]`, a `select(all)`, or one whose `groupBy` merged extra columns into the projection all produce rows that
 * can be read the same way. The type comes from the column itself -- `NativeColumn[String]` reads as a `String` -- so
 * there is no separate declaration to keep in step with the query.
 *
 * {{{
 * val result = executor.executeRows(query)
 * result.rows.map { row =>
 *   Entry(
 *     container = row.get(containerId),   // Option[String], from the column's own type
 *     value     = row(total)              // throws if absent, naming what the result did contain
 *   )
 * }
 * }}}
 *
 * Reading by name means a column must have one: native and aliased columns do, but a bare expression such as `count()`
 * takes its name from the expression it wraps and is not reliably addressable. Alias it -- `count() as "total"` -- and
 * read it back by that alias.
 */
final class Row private[execution] (val header: ResultHeader, val values: Vector[JsValue]) {

  def names: Seq[String] = header.names

  /** The undecoded value, for a column this layer has no `ColumnDecoder` for. */
  def raw(name: String): Option[JsValue] = header.indexOf(name).map(values)

  /**
   * The column's value, or `None` if the result has no such column or the value is NULL.
   *
   * Absent and NULL are deliberately not distinguished: callers overwhelmingly want "the value if there is one", and
   * [[raw]] is there for the rare case that needs to tell them apart.
   */
  def get[V](column: TableColumn[V])(implicit decoder: ColumnDecoder[V]): Option[V] = getByName[V](column.name)

  def getByName[V](name: String)(implicit decoder: ColumnDecoder[V]): Option[V] =
    raw(name).filterNot(_ == JsNull).map(decode(name, _))

  /** The column's value, failing if the result has no such column. */
  def apply[V](column: TableColumn[V])(implicit decoder: ColumnDecoder[V]): V = requiredByName[V](column.name)

  def requiredByName[V](name: String)(implicit decoder: ColumnDecoder[V]): V =
    getByName[V](name).getOrElse(
      throw ColumnLookupException(
        s"No value for column '$name'. The result has ${header.names.mkString("[", ", ", "]")}. " +
          "An expression column is named after the expression it wraps, so alias it to read it back by name."
      )
    )

  /** Every column of this row, for callers that consume a set of columns only known at runtime. */
  def fields: Map[String, JsValue] = header.names.zip(values).toMap

  private def decode[V](name: String, value: JsValue)(implicit decoder: ColumnDecoder[V]): V =
    try decoder.decode(value)
    catch {
      case cause: ColumnDecodingException =>
        val declared = header.declaredTypeOf(name).map(t => s" (declared $t)").getOrElse("")
        throw ColumnLookupException(s"Could not decode column '$name'$declared: ${cause.getMessage}", cause)
    }

  override def toString: String = fields.map { case (n, v) => s"$n=${v.compactPrint}" }.mkString("Row(", ", ", ")")
}

case class ColumnLookupException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
