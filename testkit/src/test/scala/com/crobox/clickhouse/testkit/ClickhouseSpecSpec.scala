package com.crobox.clickhouse.testkit

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpecLike, Matchers}

/**
  * @author Sjoerd Mulder
  * @since 22-10-18
  */
class ClickhouseSpecSpec extends FlatSpecLike with Matchers with ClickhouseSpec {

  override val config: Config = ConfigFactory.load()

  "ClickhouseSpec" should "have working utilities" in {
    val table = s"$database.dummy"
    clickClient.execute(s"CREATE TABLE $table (date Date) ENGINE = Memory")
    blockUntilTableExists(table)
    blockUntilTableDropped(table)

    clickClient.execute(s"CREATE TABLE $table (date Date) ENGINE = Memory")
    blockUntilTableExists(table)
    clickClient.execute(s"INSERT INTO $table VALUES ('0000-00-00')")
    clickClient.execute(s"INSERT INTO $table VALUES ('0000-00-00')")
    clickClient.execute(s"INSERT INTO $table VALUES ('0000-00-00')")

    blockUntilRowsInTable(2, table)
    blockUntilExactRowsInTable(3, table)

    dropAllTables() should be (1)

    dropAllTables() should be (0)

  }

}
