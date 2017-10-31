package com.crobox.clickhouse

import akka.http.scaladsl.model.Uri

package object discovery {
  //  TODO we might want to provide the ability to specify a different port when using the hostnames from the cluster table
  case class ConnectionConfig(host: Uri, cluster: String)
}
