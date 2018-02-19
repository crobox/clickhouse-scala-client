import java.util.UUID

import com.crobox.clickhouse.dsl._
import java.util.UUID

import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizerModule}
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.dsl.marshalling.ClickhouseJsonSupport._
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import org.joda.time.DateTime
import spray.json._

trait TestSchema {
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
  implicit val tokenizerDatabase: TokenizerModule.Database = "default"

  case object OneTestTable extends Table {
    override val name: String                   = "captainAmerica"
    override val columns: List[NativeColumn[_]] = List(shieldId, timestampColumn)
  }

  case object TwoTestTable extends Table {
    override val name: String                   = "twoTestTable"
    override val columns: List[NativeColumn[_]] = List(itemId, col1, col2, col3, col4)
  }

  val shieldId        = NativeColumn[UUID]("shield_id")
  val itemId          = NativeColumn[UUID]("item_id")
  val col1            = NativeColumn[String]("column_1")
  val col2            = NativeColumn[Int]("column_2", ColumnType.UInt32)
  val col3            = NativeColumn[String]("column_3")
  val col4            = NativeColumn[String]("column_4")
  val timestampColumn = NativeColumn[Long]("ts", ColumnType.UInt64)

  case class Table1Entry(shieldId: UUID, date: DateTime = DateTime.now())

  object Table1Entry {

    object Format extends JsonWriter[Table1Entry] {

      override def write(obj: Table1Entry): JsValue =
        JsObject(
          "shield_id" -> JsString(obj.shieldId.toString),
          "ts"        -> JsNumber(obj.date.getMillis)
        )
    }
    val reader: RootJsonFormat[Table1Entry] = jsonFormat(Table1Entry.apply, "shield_id", "ts")
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

object TestSchema {
  case class TestTable(override val name: String, override val columns: List[NativeColumn[_]]) extends Table
}



object TestDSL extends TestSchema with App with ClickhouseTokenizerModule {
  implicit val database: Database = "moo"
  val q = select(shieldId as itemId) from OneTestTable where shieldId.isEq(UUID.randomUUID())

  println(toSql(q.internalQuery))
}



