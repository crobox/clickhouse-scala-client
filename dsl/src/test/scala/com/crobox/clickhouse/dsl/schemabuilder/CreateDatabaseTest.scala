package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslTestSpec

/**
 * @author Sjoerd Mulder
 * @since 2-1-17
 */
class CreateDatabaseTest extends DslTestSpec {

  it should "deny creating invalid databases" in {
    intercept[IllegalArgumentException](
      CreateDatabase("").toString
    )
  }

  it should "create a database with invalid name" in {
    CreateDatabase(".Fool").toString should be("CREATE DATABASE `.Fool`")
  }

  it should "create a database with ON CLUSTER clause" in {
    CreateDatabase("db", clusterName = Option("test_cluster")).toString should be(
      "CREATE DATABASE db ON CLUSTER test_cluster"
    )
  }

  it should "quote database creation with an invalid cluster name" in {
    CreateDatabase("db", clusterName = Option(".Invalid")).toString should be(
      "CREATE DATABASE db ON CLUSTER `.Invalid`"
    )
  }
}
