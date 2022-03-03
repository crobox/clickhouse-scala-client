package com.crobox

package object clickhouse {
  case class ClickhouseServerVersion(versions: Seq[Int]) {

    def minimalVersion(version: Int): Boolean =
      versions.head >= version

    def minimalVersion(version: Int, subVersion: Int): Boolean =
      versions.head >= version && versions(1) >= subVersion
  }

  object ClickhouseServerVersion {

    def apply(version: String): ClickhouseServerVersion =
      ClickhouseServerVersion(version.split('.').toSeq.map(_.filter(_.isDigit)).map(_.toInt))
  }
}
