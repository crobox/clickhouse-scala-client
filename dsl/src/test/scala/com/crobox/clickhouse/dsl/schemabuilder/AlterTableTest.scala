package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl.NativeColumn
import com.crobox.clickhouse.dsl.schemabuilder.ColumnOperation.{AddColumn, DropColumn, ModifyColumn}

/**
 * @author Sjoerd Mulder
 * @since 2-1-17
 */
class AlterTableTest extends DslTestSpec {

  it should "create various alter tables" in {
    AlterTable(
      "a",
      Seq(
        AddColumn(NativeColumn("b")),
        AddColumn(NativeColumn("e", ColumnType.UInt32), Some("d")),
        DropColumn("c"),
        ModifyColumn(NativeColumn("d", ColumnType.String))
      )
    ).query should be(
      "ALTER TABLE a ADD COLUMN b String, ADD COLUMN e UInt32 AFTER d, DROP COLUMN c, MODIFY COLUMN d String"
    )
  }
}
