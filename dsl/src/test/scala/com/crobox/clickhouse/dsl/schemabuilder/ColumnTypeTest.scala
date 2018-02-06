package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.NativeColumn
import org.scalatest.{FlatSpecLike, Matchers}

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
class ColumnTypeTest extends FlatSpecLike with Matchers {

  it should "allow Nested types" in {
    ColumnType.Nested(NativeColumn("a"), NativeColumn("b", ColumnType.Int8)).toString should be(
      "Nested(a String, b Int8)"
    )
  }

  it should "deny double Nesting" in {
    intercept[IllegalArgumentException] {
      ColumnType.Nested(NativeColumn("a", ColumnType.Nested(NativeColumn("b"))))
    }
  }

}
