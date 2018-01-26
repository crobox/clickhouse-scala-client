package com.crobox.clickhouse.dsl

import org.scalatest.{FlatSpecLike, Matchers}

/**
 * @author Sjoerd Mulder
 * @since 11-1-17
 */
class ClickhouseStatementTest extends FlatSpecLike with Matchers {

  it should "escape values" in {
    ClickhouseStatement.escape(null) should be("NULL")
    ClickhouseStatement.escape("\\-\n-\t-\b-\f-\r-\u0000-\'-`-foo") should be(
      "\\\\-\\n-\\t-\\b-\\f-\\r-\\0-\\'-\\`-foo"
    )
  }

}
