package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.ClickhouseStatement

trait DistributedDdlSupport {

  val clusterName : Option[String]

  def requireValidCluster(errorMsg : String): Unit = {
    require(clusterName.forall(ClickhouseStatement.isValidIdentifier), errorMsg)
  }

  protected[schemabuilder] def printOnCluster() : String = {
    clusterName.map(cluster => s" ON CLUSTER $cluster").getOrElse("")
  }

}
