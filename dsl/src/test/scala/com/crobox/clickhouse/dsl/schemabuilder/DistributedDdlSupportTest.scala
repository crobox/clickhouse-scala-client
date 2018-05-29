package com.crobox.clickhouse.dsl.schemabuilder

import org.scalatest.{FlatSpec, Matchers}

class DistributedDdlSupportTest extends FlatSpec with Matchers {

  class Dummy(val clusterName: Option[String]) extends DistributedDdlSupport

  it should "consider None as a valid cluster and NOT print ON CLUSTER statement" in {
    val underTest = new Dummy(None)

    underTest.requireValidCluster("")
    underTest.printOnCluster() should be ("")
  }

  it should "consider Some(\"valid_cluster\") as a valid cluster and print ON CLUSTER statement" in {
    val underTest = new Dummy(Some("valid_cluster"))

    underTest.requireValidCluster("")
    underTest.printOnCluster() should be (" ON CLUSTER valid_cluster")
  }

  it should "reject an invalid cluster name" in {
    an[IllegalArgumentException] should be thrownBy {
      new Dummy(Some("")).requireValidCluster("")
    }

    an[IllegalArgumentException] should be thrownBy {
      new Dummy(Some(null)).requireValidCluster("")
    }

    an[IllegalArgumentException] should be thrownBy {
      new Dummy(Some("9aze")).requireValidCluster("")
    }

    an[IllegalArgumentException] should be thrownBy {
      new Dummy(Some("aze@")).requireValidCluster("")
    }
  }
}
