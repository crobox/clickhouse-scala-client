package com.crobox.clickhouse.dsl.schemabuilder

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

  it should "create a database with ON CLUSTER clause" in {
    CreateDatabase("db", clusterName = Option("test_cluster")).toString should be ("CREATE DATABASE db ON CLUSTER test_cluster")
  }

  it should "reject database creation with an invalid cluster name" in {
    an[IllegalArgumentException] should be thrownBy {
      CreateDatabase("db", clusterName = Option(".Invalid"))
    }
  }

}
