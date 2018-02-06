package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.UnderlyingQuery
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database

object TokenizerModule {
  type Database = String
}

trait TokenizerModule {

  def toSql(query: UnderlyingQuery, formatting: Option[String] = Some("JSON"))(implicit database: Database): String
}
