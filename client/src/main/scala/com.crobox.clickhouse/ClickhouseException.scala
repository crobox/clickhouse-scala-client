package com.crobox.clickhouse

import akka.http.scaladsl.model.StatusCode

/**
 * @author Sjoerd Mulder
 * @since 31-03-17
 */
case class ClickhouseException(message: String, query: String, cause: Throwable = null, statusCode: StatusCode)
    extends RuntimeException(message + s", query $query", cause)
case class ClickhouseChunkedException(message: String) extends RuntimeException(message)
