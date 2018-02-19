package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

case class SelectQuery(columns: Seq[AnyTableColumn], modifier: String = "") extends Query with OperationalQuery {
  override val internalQuery = InternalQuery(Some(this))

}


