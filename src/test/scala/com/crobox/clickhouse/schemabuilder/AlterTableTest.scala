package com.crobox.clickhouse.schemabuilder

import com.crobox.clickhouse.schemabuilder.ColumnOperation.{AddColumn, DropColumn, ModifyColumn}
import org.scalatest.{FlatSpecLike, Matchers}

/**
 * @author Sjoerd Mulder
 * @since 2-1-17
 */
class AlterTableTest extends FlatSpecLike with Matchers {

  it should "create various alter tables" in {
    AlterTable("a",
               Seq(
                 AddColumn(Column("b")),
                 AddColumn(Column("e", ColumnType.Int), Some("d")),
                 DropColumn("c"),
                 ModifyColumn(Column("d", ColumnType.String))
               )).query should be(
      "ALTER TABLE a ADD COLUMN b String, ADD COLUMN e UInt32 AFTER d, DROP COLUMN c, MODIFY COLUMN d String"
    )

  }

}
