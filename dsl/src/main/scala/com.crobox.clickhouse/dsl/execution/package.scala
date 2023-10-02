package com.crobox.clickhouse.dsl

import spray.json.{JsonReader, _}

package object execution {

  case class Statistic(rowsRead: Long, rowsBeforeLimit: Long)

  case class ResultColumnType(name: String, columnType: String)

  case class ResultMeta(columnTypes: Seq[ResultColumnType])

  case class QueryResult[V](rows: Seq[V], meta: Option[ResultMeta] = None, statistic: Option[Statistic] = None) {

    def size: Int = rows.size
  }

  object QueryResult {

    implicit def format[V: JsonReader]: JsonReader[QueryResult[V]] = (json: JsValue) => {
      val jsObject = json.asJsObject
      val rows = jsObject.getFields("data") match {
        case Seq(JsArray(results)) => results.map(_.convertTo[V])
      }
      val meta: Option[ResultMeta] = jsObject.fields.get("meta").flatMap {
        case JsArray(columnDefinitions) =>
          Option(ResultMeta(columnDefinitions.map(_.asJsObject.getFields("name", "type") match {
            case Seq(JsString(name), JsString(colType)) => ResultColumnType(name, colType)
          })))
        case _ => None
      }
      val statistic = jsObject.getFields("rows_before_limit_at_least", "rows") match {
        case Seq(JsNumber(limit), JsNumber(rowsRead)) => Some(Statistic(rowsRead.longValue, limit.longValue))
        case _                                        => None
      }
      QueryResult(rows, meta, statistic)
    }
  }
}
