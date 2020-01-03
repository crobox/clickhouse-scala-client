package com.crobox.clickhouse.dsl

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

/**
 * @author Sjoerd Mulder
 * @since 11-1-17
 */
class ClickhouseStatementTest extends AnyFlatSpecLike with Matchers {

  it should "escape values" in {
    ClickhouseStatement.escape(null) should be("NULL")
    ClickhouseStatement.escape("\\-\n-\t-\b-\f-\r-\u0000-\'-`-foo") should be(
      "\\\\-\\n-\\t-\\b-\\f-\\r-\\0-\\'-\\`-foo"
    )
  }

}
