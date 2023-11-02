package com.crobox.clickhouse

import org.apache.pekko.http.scaladsl.model.StatusCode

sealed abstract class ClickhouseExecutionException(msg: String, cause: Throwable = null)
    extends RuntimeException(msg, cause) {
  val retryable: Boolean
}

case class ClickhouseException(message: String, query: String, cause: Throwable = null, statusCode: StatusCode)
    extends ClickhouseExecutionException(message + s", query $query", cause) {
  override val retryable: Boolean = true
}

case class ClickhouseChunkedException(message: String) extends ClickhouseExecutionException(message) {
  override val retryable: Boolean = true
}

case class TooManyQueriesException()
    extends ClickhouseExecutionException(
      "The client's queue is full, you are trying to execute too many queries at the same time. This can be solved by either: checking the source of the queries to make sure this is not a bug\n Increasing the buffer size under the property `crobox.clickhouse.client.buffer-size`\n Adjust the settings of the super pool under `pekko.http.host-connection-pool`"
    ) {
  override val retryable: Boolean = false
}
