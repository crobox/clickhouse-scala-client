package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

/**
 * @author Sjoerd Mulder
 * @since 11-1-17
 */
class ClickhouseStatementTest extends DslTestSpec {

  it should "escape values" in {
    ClickhouseStatement.escape(null) should be("NULL")
    ClickhouseStatement.escape("\\-\n-\t-\b-\f-\r-\u0000-\'-`-foo") should be(
      "\\\\-\\n-\\t-\\b-\\f-\\r-\\0-\\'-\\`-foo"
    )
  }
}
