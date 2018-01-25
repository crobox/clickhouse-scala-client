package com.crobox.clickhouse.query

import com.crobox.clickhouse.query.TokenizerModule.Database

object TokenizerModule {
  type Database = String
}

trait TokenizerModule {

  def toSql(query: UnderlyingQuery, formatting: Option[String] = Some("JSON"))(implicit database: Database): String
}
