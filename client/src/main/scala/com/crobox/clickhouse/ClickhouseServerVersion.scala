package com.crobox.clickhouse

case class ClickhouseServerVersion(versions: Seq[Int]) {

  def minimalVersion(version: Int): Boolean = versions.head >= version

  def minimalVersion(version: Int, subVersion: Int): Boolean =
    if (versions.head < version) false
    else if (versions.head == version) versions(1) >= subVersion
    else true
}

object ClickhouseServerVersion {

  def apply(version: String): ClickhouseServerVersion =
    ClickhouseServerVersion(version.split('.').toSeq.map(_.filter(_.isDigit)).filter(_.trim.nonEmpty).map(_.toInt))

  /**
   * Assumed when the server's version cannot be determined. Not actually the latest release -- it is a floor, chosen as
   * the oldest ClickHouse LTS this client is tested against, so that version-gated SQL generation defaults to what a
   * supported server understands. Guessing is unavoidable here because tokenization needs a version synchronously.
   */
  def fallback: ClickhouseServerVersion = ClickhouseServerVersion(versions = Seq(25, 3, 0))

  @deprecated("Renamed to `fallback`; it never meant the latest release. Will be removed in 2.0.", "1.3.1")
  def latest: ClickhouseServerVersion = fallback
}
