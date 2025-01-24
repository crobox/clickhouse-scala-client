package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.ClickhouseStatement

/**
 * @author
 *   Sjoerd Mulder
 * @since 2-1-17
 */
abstract class ClickhouseSchemaStatement extends ClickhouseStatement {

  protected def printIfNotExists(v: Boolean): String =
    if (v) {
      " IF NOT EXISTS"
    } else {
      ""
    }

  override def toString: String = query

}
