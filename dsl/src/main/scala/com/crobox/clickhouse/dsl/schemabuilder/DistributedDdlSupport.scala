package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.ClickhouseStatement

trait DistributedDdlSupport {

  val clusterName: Option[String]

  protected[schemabuilder] def printOnCluster(): String =
    clusterName.map(cluster => s" ON CLUSTER ${ClickhouseStatement.quoteIdentifier(cluster)}").getOrElse("")

}
