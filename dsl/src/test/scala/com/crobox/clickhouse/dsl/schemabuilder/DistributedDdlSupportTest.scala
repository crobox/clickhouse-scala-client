package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslTestSpec

class DistributedDdlSupportTest extends DslTestSpec {

  class Dummy(val clusterName: Option[String]) extends DistributedDdlSupport

  it should "consider None as a valid cluster and NOT print ON CLUSTER statement" in {
    val underTest = new Dummy(None)

    underTest.printOnCluster() should be("")
  }

  it should "consider Some(\"valid_cluster\") as a valid cluster and print ON CLUSTER statement" in {
    val underTest = new Dummy(Some("valid_cluster"))

    underTest.printOnCluster() should be(" ON CLUSTER valid_cluster")
  }

  it should "consider Some(\";DROP TABLE students;\") as a valid cluster and print ON CLUSTER statement" in {
    val underTest = new Dummy(Some(";DROP TABLE students;"))

    underTest.printOnCluster() should be(" ON CLUSTER `;DROP TABLE students;`")
  }

}
