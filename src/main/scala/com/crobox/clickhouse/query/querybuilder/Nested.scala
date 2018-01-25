package com.crobox.clickhouse.query.querybuilder

import com.crobox.clickhouse.ClickhouseStatement

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
object Nested {

  def hasKey(key: String, property: String = "props"): String =
    s"has($property.key, '${ClickhouseStatement.escape(key)}')"

  def valueLookup(key: String, property: String = "props"): String =
    s"$property.value[indexOf($property.key, '${ClickhouseStatement.escape(key)}')]"

}
