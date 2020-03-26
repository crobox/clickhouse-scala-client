package com.crobox.clickhouse.dsl

case class SelectQuery(columns: Seq[Column], modifier: String = "") extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(Some(this))

}


