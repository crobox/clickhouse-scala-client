package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.InternalQuery
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database

object TokenizerModule {
  type Database = String
}

trait TokenizerModule {

  def toSql(query: InternalQuery, formatting: Option[String] = Some("JSON"))(implicit database: Database): String
}
