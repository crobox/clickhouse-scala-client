package com.crobox.clickhouse.dsl.column

import java.util.UUID

import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

trait DictionaryFunctions { self: Magnets =>

  sealed abstract class DictionaryGetFuncColumn[V](val dictName: StringColMagnet, val attrName: StringColMagnet, val id: NumericCol, val default: Option[TableColumn[V]] = None) extends
    DictionaryFuncColumn[V]

  sealed abstract class DictionaryFuncColumn[V] extends
    ExpressionColumn[V](EmptyColumn())

  case class DictGetUInt8(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetUInt16(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetUInt32(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetUInt64(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetInt8(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetInt16(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetInt32(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetInt64(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Long]] = None)
    extends DictionaryGetFuncColumn[Long](_dictName,_attrName,_id,_default)
  case class DictGetFloat32(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Float]] = None)
    extends DictionaryGetFuncColumn[Float](_dictName,_attrName,_id,_default)
  case class DictGetFloat64(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[Float]] = None)
    extends DictionaryGetFuncColumn[Float](_dictName,_attrName,_id,_default)
  case class DictGetDate(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[LocalDate]] = None)
    extends DictionaryGetFuncColumn[LocalDate](_dictName,_attrName,_id,_default)
  case class DictGetDateTime(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[DateTime]] = None)
    extends DictionaryGetFuncColumn[DateTime](_dictName,_attrName,_id,_default)
  case class DictGetUUID(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[UUID]] = None)
    extends DictionaryGetFuncColumn[UUID](_dictName,_attrName,_id,_default)
  case class DictGetString(_dictName: StringColMagnet, _attrName: StringColMagnet, _id: NumericCol, _default: Option[TableColumn[String]] = None)
    extends DictionaryGetFuncColumn[String](_dictName,_attrName,_id,_default)

  case class DictIsIn(dictName: StringColMagnet, childId: NumericCol, ancestorId: NumericCol) extends DictionaryFuncColumn[Boolean]
  case class DictGetHierarchy(dictName: StringColMagnet, id: NumericCol) extends DictionaryFuncColumn[String]
  case class DictHas(dictName: StringColMagnet, id: NumericCol) extends DictionaryFuncColumn[Boolean]


  //todo implement '...orDefault'
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
