package com.crobox.clickhouse.query.schemabuilder

import org.scalatest.{FlatSpecLike, Matchers}

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
class ColumnTypeTest extends FlatSpecLike with Matchers {

  it should "allow Nested types" in {
    ColumnType.Nested(Column("a"), Column("b", ColumnType.Int8)).toString should be(
      "Nested(a String, b Int8)"
    )
  }

  it should "deny double Nesting" in {
    intercept[IllegalArgumentException] {
      ColumnType.Nested(Column("a", ColumnType.Nested(Column("b"))))
    }
  }

}
