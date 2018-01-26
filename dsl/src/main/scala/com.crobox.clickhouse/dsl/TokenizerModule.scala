package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TokenizerModule.Database

object TokenizerModule {
  type Database = String
}

trait TokenizerModule {

  def toSql(query: UnderlyingQuery, formatting: Option[String] = Some("JSON"))(implicit database: Database): String
}
