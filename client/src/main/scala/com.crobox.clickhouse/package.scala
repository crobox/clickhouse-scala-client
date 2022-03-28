package com.crobox

package object clickhouse {
  case class ClickhouseServerVersion(versions: Seq[Int]) {

    def minimalVersion(version: Int): Boolean = versions.head >= version

    def minimalVersion(version: Int, subVersion: Int): Boolean =
      if (versions.head < version) false
      else if (versions.head == version) versions(1) >= subVersion
      else true
  }

  object ClickhouseServerVersion {

    def apply(version: String): ClickhouseServerVersion =
      ClickhouseServerVersion(version.split('.').toSeq.map(_.filter(_.isDigit)).map(_.toInt))

    def latest: ClickhouseServerVersion = ClickhouseServerVersion(versions = Seq(21, 8, 14))
  }
}
