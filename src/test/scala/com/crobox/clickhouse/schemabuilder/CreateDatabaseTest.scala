package com.crobox.clickhouse.schemabuilder

import org.scalatest.{FlatSpecLike, Matchers}

/**
 * @author Sjoerd Mulder
 * @since 2-1-17
 */
class CreateDatabaseTest extends FlatSpecLike with Matchers {

  it should "deny creating invalid databases" in {
    intercept[IllegalArgumentException](
      CreateDatabase("")
    )
    intercept[IllegalArgumentException](
      CreateDatabase(".Fool")
    )

  }

}
