package com.crobox.clickhouse.dsl.execution

import com.crobox.clickhouse.dsl.marshalling.RowDecoder
import spray.json._

/**
 * Parses `JSONCompactEachRowWithNamesAndTypes`: a line of column names, a line of declared ClickHouse types, then one
 * JSON array per row.
 *
 * Chosen over `FORMAT JSON` for three reasons. It is line-delimited, so it can eventually be consumed incrementally
 * rather than buffered whole (see #75). It is positional, which is what removes the string coupling between a query's
 * aliases and its decoder's field names. And it states each column's declared type, so decoding no longer has to guess
 * how a value was spelled -- which is what makes pinning `output_format_json_quote_64bit_integers` unnecessary here.
 */
private[dsl] object CompactRowParser {

  val Format: String = "JSONCompactEachRowWithNamesAndTypes"

  def parse[R](body: String, decoder: RowDecoder[R]): QueryResult[R] = {
    val lines = body.linesIterator.filter(_.trim.nonEmpty).toVector

    if (lines.size < 2)
      throw ResultParsingException(
        s"Expected a header of column names and types from $Format, got ${lines.size} non-empty line(s)"
      )

    val names = stringsOf(lines.head, "names")
    val types = stringsOf(lines(1), "types")

    if (decoder.arity != names.size)
      throw ResultParsingException(
        s"Query returns ${names.size} column(s) ${names.mkString("[", ", ", "]")} but the decoder expects " +
          s"${decoder.arity}. The select list and the result type have to agree."
      )

    val rows = lines.drop(2).map { line =>
      val values = line.parseJson match {
        case JsArray(elements) => elements
        case other             => throw ResultParsingException(s"Expected a row array, got ${other.compactPrint}")
      }
      if (values.size != names.size)
        throw ResultParsingException(s"Row has ${values.size} value(s), header declares ${names.size}")
      decoder.decode(values, names)
    }

    // No statistic: unlike FORMAT JSON's envelope, this format carries no `rows_before_limit_at_least`.
    QueryResult(rows, Option(ResultMeta(names.zip(types).map { case (n, t) => ResultColumnType(n, t) })), None)
  }

  private def stringsOf(line: String, what: String): Vector[String] =
    line.parseJson match {
      case JsArray(elements) =>
        elements.map {
          case JsString(s) => s
          case other       =>
            throw ResultParsingException(s"Expected a string in the $what header, got ${other.compactPrint}")
        }.toVector
      case other => throw ResultParsingException(s"Expected the $what header to be an array, got ${other.compactPrint}")
    }
}

case class ResultParsingException(message: String) extends RuntimeException(message)
