package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.dsl.marshalling.ClickhouseJsonSupport._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import org.joda.time.DateTime
import spray.json._

trait TestSchema {

  val database: String

  implicit object UUIDFormat extends JsonFormat[UUID] {

    def write(obj: UUID): JsValue = {
      require(obj ne null)
      JsString(obj.toString)
    }

    def read(json: JsValue): UUID = json match {
      case JsString(s) =>
        try {
          UUID.fromString(s)
        } catch {
          case t: Throwable => deserializationError(s"Invalid UUID '$s' found", t)
        }
      case _ => deserializationError("String expected")
    }
  }

  private lazy val _db = database
  case object OneTestTable extends Table {
    override lazy val database: String          = _db
    override val name: String                   = "captainAmerica"
    override val columns: List[NativeColumn[_]] = List(shieldId, timestampColumn, numbers)
  }

  case object TwoTestTable extends Table {
    override lazy val database: String          = _db
    override val name: String                   = "twoTestTable"
    override val columns: List[NativeColumn[_]] = List(itemId, col1, col2, col3, col4, nativeUUID)
  }

  case object ThreeTestTable extends Table {
    override lazy val database: String          = _db
    override val name: String                   = "threeTestTable"
    override val columns: List[NativeColumn[_]] = List(itemId, col2, col4, col5, col6)
  }

  val shieldId        = NativeColumn[UUID]("shield_id")
  val itemId          = NativeColumn[UUID]("item_id")
  val numbers         = NativeColumn[Seq[Int]]("numbers", ColumnType.Array(ColumnType.UInt32))
  val col1            = NativeColumn[String]("column_1")
  val col2            = NativeColumn[Int]("column_2", ColumnType.UInt32)
  val col3            = NativeColumn[String]("column_3")
  val col4            = NativeColumn[String]("column_4")
  val col5            = NativeColumn[String]("column_5")
  val col6            = NativeColumn[String]("column_6")
  val timestampColumn = NativeColumn[Long]("ts", ColumnType.UInt64)
  val nativeUUID      = NativeColumn[UUID]("uuid", ColumnType.UUID)

  case class Table1Entry(shieldId: UUID, date: DateTime = DateTime.now(), numbers: Seq[Int] = Seq())

  object Table1Entry {

    object Format extends JsonWriter[Table1Entry] {

      override def write(obj: Table1Entry): JsValue =
        JsObject(
          "shield_id" -> JsString(obj.shieldId.toString),
          "ts"        -> JsNumber(obj.date.getMillis),
          "numbers"   -> JsArray(obj.numbers.map(JsNumber(_)).toVector)
        )
    }
    val reader: RootJsonFormat[Table1Entry] = jsonFormat(Table1Entry.apply, "shield_id", "ts", "numbers")
    implicit val format                     = jsonFormat(reader, Format)
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
