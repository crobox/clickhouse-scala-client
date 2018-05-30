package com.crobox.clickhouse.dsl.column

import java.util.UUID

import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

trait DictionaryFunctions { self: Magnets =>

  sealed abstract class DictionaryColumn[V](dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[V]] = None) extends
    ExpressionColumn[V](EmptyColumn())

  case class DictGetUInt8(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetUInt16(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetUInt32(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetUInt64(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetInt8(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetInt16(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetInt32(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetInt64(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Long]] = None)
    extends DictionaryColumn[Long](dictName,attrName,id,default)
  case class DictGetFloat32(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Float]] = None)
    extends DictionaryColumn[Float](dictName,attrName,id,default)
  case class DictGetFloat64(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[Float]] = None)
    extends DictionaryColumn[Float](dictName,attrName,id,default)
  case class DictGetDate(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[LocalDate]] = None)
    extends DictionaryColumn[LocalDate](dictName,attrName,id,default)
  case class DictGetDateTime(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[DateTime]] = None)
    extends DictionaryColumn[DateTime](dictName,attrName,id,default)
  case class DictGetUUID(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[UUID]] = None)
    extends DictionaryColumn[UUID](dictName,attrName,id,default)
  case class DictGetString(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol, default: Option[TableColumn[String]] = None)
    extends DictionaryColumn[String](dictName,attrName,id,default)

  case class DictIsIn(dictName: StringColMagnet, childId: NumericCol, ancestorId: NumericCol)
  case class DictGetHierarchy(dictName: StringColMagnet, id: NumericCol)
  case class DictHas(dictName: StringColMagnet, id: NumericCol)

  def dictGetUInt8(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetUInt8(dictName, attrName, id)
  def dictGetUInt16(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetUInt16(dictName, attrName, id)
  def dictGetUInt32(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetUInt32(dictName, attrName, id)
  def dictGetUInt64(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetUInt64(dictName, attrName, id)
  def dictGetInt8(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetInt8(dictName, attrName, id)
  def dictGetInt16(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetInt16(dictName, attrName, id)
  def dictGetInt32(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetInt32(dictName, attrName, id)
  def dictGetInt64(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetInt64(dictName, attrName, id)
  def dictGetFloat32(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetFloat32(dictName, attrName, id)
  def dictGetFloat64(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetFloat64(dictName, attrName, id)
  def dictGetDate(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetDate(dictName, attrName, id)
  def dictGetDateTime(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetDateTime(dictName, attrName, id)
  def dictGetUUID(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetUUID(dictName, attrName, id)
  def dictGetString(dictName: StringColMagnet, attrName: StringColMagnet, id: NumericCol) = DictGetString(dictName, attrName, id)
  def dictIsIn(dictName: StringColMagnet, childId: NumericCol, id: NumericCol) = DictIsIn(dictName, childId, id)
  def dictGetHierarchy(dictName: StringColMagnet, id: NumericCol) = DictGetHierarchy(dictName, id)
  def dictHas(dictName: StringColMagnet, id: NumericCol) = DictHas(dictName, id)
/*
dictGetUInt8
dictGetUInt16
dictGetUInt32
dictGetUInt64
dictGetInt8
dictGetInt16
dictGetInt32
dictGetInt64
dictGetFloat32
dictGetFloat64
dictGetDate
dictGetDateTime
dictGetUUID
dictGetString
dictGetTOrDefault
dictIsIn
dictGetHierarchy
dictHas
 */
}
