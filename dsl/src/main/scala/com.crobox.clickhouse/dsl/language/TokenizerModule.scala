package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.InternalQuery

trait TokenizerModule {
  def toSql(query: InternalQuery, formatting: Option[String] = Some("JSON")): String
}
