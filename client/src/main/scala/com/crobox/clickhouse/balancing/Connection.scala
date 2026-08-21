package com.crobox.clickhouse.balancing

object Connection {

  sealed trait ConnectionType

  object ConnectionType {

    def apply(connectionType: String): ConnectionType =
      connectionType match {
        case "single-host"     => SingleHost
        case "balancing-hosts" => BalancingHosts
        case "cluster-aware"   => ClusterAware
        case unknown           =>
          throw new IllegalArgumentException(
            s"Unknown crobox.clickhouse.client.connection.type [$unknown]; " +
              "expected one of single-host, balancing-hosts, cluster-aware"
          )
      }
  }

  case object SingleHost extends ConnectionType

  case object BalancingHosts extends ConnectionType

  case object ClusterAware extends ConnectionType

}
