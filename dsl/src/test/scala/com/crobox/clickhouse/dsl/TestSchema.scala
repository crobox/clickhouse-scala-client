package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.dsl.marshalling.JsonProtocol._
import org.joda.time.DateTime
import spray.json._

trait TestSchema {
  val r                                                    = scala.util.Random
  implicit val tokenizerDatabase: TokenizerModule.Database = "default"

  trait OneTable extends Table

  trait TwoTable extends Table

  case object OneTestTable extends OneTable {
    override val name: String = "captainAmerica"
  }

  case object TwoTestTable extends TwoTable {
    override val name: String = "twoTestTable"
  }

  trait VirtualOneWithTwo extends OneTable with TwoTable

  case class ShieldId() extends TableColumn[UUID]("shield_id")

  case class ItemId() extends TableColumn[UUID]("item_id")

  case class Table2Column1() extends TableColumn[String]("column_1")

  case class Table2Column2() extends TableColumn[Int]("column_2")

  case class Table2Column3() extends TableColumn[String]("column_3")

  case class Table2Column4() extends TableColumn[Option[String]]("column_4")

  val shieldId        = ShieldId()
  val itemId          = ItemId()
  val col1            = Table2Column1()
  val col2            = Table2Column2()
  val col3            = Table2Column3()
  val col4            = Table2Column4()
  val timestampColumn = new TableColumn[Long]("ts") {}

  case class Table1Entry(shieldId: UUID, date: DateTime = DateTime.now())

  object Table1Entry {

    object Format extends JsonWriter[Table1Entry] {

      override def write(obj: Table1Entry): JsValue =
        JsObject(
          "shield_id" -> JsString(obj.shieldId.toString),
          "ts"        -> JsNumber(obj.date.getMillis)
        )
    }
    val reader          = jsonFormat(Table1Entry.apply, "shield_id", "ts")
    implicit val format = jsonFormat(reader, Format)
  }

  case class Table2Entry(itemId: UUID,
                         firstColumn: String,
                         secondColumn: Int,
                         thirdColumn: String,
                         forthColumn: Option[String])

  object Table2Entry {
    implicit val entry2Format =
      jsonFormat(Table2Entry.apply, "item_id", "column_1", "column_2", "column_3", "column_4")
  }
}
