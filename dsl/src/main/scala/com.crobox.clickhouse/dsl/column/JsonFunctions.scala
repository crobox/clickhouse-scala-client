package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait JsonFunctions { self: Magnets =>
  abstract class JsonFunction[T](val params: StringColMagnet[_], val fieldName: StringColMagnet[_]) extends ExpressionColumn[T](params.column)

  case class VisitParamHas(_params: StringColMagnet[_], _fieldName: StringColMagnet[_])   extends JsonFunction[Boolean](_params, _fieldName)
  case class VisitParamExtractUInt(_params: StringColMagnet[_], _fieldName: StringColMagnet[_]) extends JsonFunction[Long](_params, _fieldName)
  case class VisitParamExtractInt(_params: StringColMagnet[_], _fieldName: StringColMagnet[_]) extends JsonFunction[Long](_params, _fieldName)
  case class VisitParamExtractFloat(_params: StringColMagnet[_], _fieldName: StringColMagnet[_]) extends JsonFunction[Float](_params, _fieldName)
  case class VisitParamExtractBool(_params: StringColMagnet[_], _fieldName: StringColMagnet[_]) extends JsonFunction[Boolean](_params, _fieldName)
  case class VisitParamExtractRaw[T](_params: StringColMagnet[_], _fieldName: StringColMagnet[_]) extends JsonFunction[T](_params, _fieldName)
  case class VisitParamExtractString(_params: StringColMagnet[_], _fieldName: StringColMagnet[_]) extends JsonFunction[String](_params, _fieldName)

  def visitParamHas(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =                  VisitParamHas(params, fieldName)
  def visitParamExtractUInt(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =          VisitParamExtractUInt(params, fieldName)
  def visitParamExtractInt(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =           VisitParamExtractInt(params, fieldName)
  def visitParamExtractFloat(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =         VisitParamExtractFloat(params, fieldName)
  def visitParamExtractBool(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =          VisitParamExtractBool(params, fieldName)
  def visitParamExtractRaw(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =           VisitParamExtractRaw(params, fieldName)
  def visitParamExtractString(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =        VisitParamExtractString(params, fieldName)

  /*
visitParamHas(params, name)
visitParamExtractUInt(params, name)
visitParamExtractInt(params, name)
visitParamExtractFloat(params, name)
visitParamExtractBool(params, name)
visitParamExtractRaw(params, name)
visitParamExtractString(params, name)
 */
}
