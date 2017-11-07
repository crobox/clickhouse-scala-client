package com.crobox.clickhouse

/**
 * @author Sjoerd Mulder
 * @since 31-03-17
 */
class ClickhouseException(message: String, query: String, cause: Throwable = null)
    extends RuntimeException(message + s", query $query", cause)
