package com.crobox.clickhouse.internal

import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * Reads a duration out of config at millisecond resolution.
 *
 * This existed as `getDuration(path).getSeconds.seconds` written out at four separate sites, which floored anything
 * below a second -- a `500 millis` health-check interval became `0`, a tight polling loop. Correcting four hand-written
 * copies of the same expression is also how one of them ended up as `.toMillis.seconds`, inflating every value a
 * thousandfold, so the expression now lives in one place with a test on it.
 */
private[clickhouse] object ConfigDuration {

  def apply(config: Config, path: String): FiniteDuration =
    config.getDuration(path).toMillis.millis

  /** For settings a caller may not have in scope at all; see HostBalancer.hostRetrievalTimeout. */
  def orElse(config: Config, path: String, default: FiniteDuration): FiniteDuration =
    if (config.hasPath(path)) apply(config, path) else default
}
