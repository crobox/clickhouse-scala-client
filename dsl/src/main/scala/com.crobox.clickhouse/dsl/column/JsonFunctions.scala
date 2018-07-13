package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait JsonFunctions { self: Magnets =>
  abstract class JsonFunction[T](val params: StringColMagnet, val fieldName: StringColMagnet) extends ExpressionColumn[T](params.column)

  case class VisitParamHas(_params: StringColMagnet, _fieldName: StringColMagnet)   extends JsonFunction[Boolean](_params, _fieldName)
  case class VisitParamExtractUInt(_params: StringColMagnet, _fieldName: StringColMagnet) extends JsonFunction[Long](_params, _fieldName)
  case class VisitParamExtractInt(_params: StringColMagnet, _fieldName: StringColMagnet) extends JsonFunction[Long](_params, _fieldName)
  case class VisitParamExtractFloat(_params: StringColMagnet, _fieldName: StringColMagnet) extends JsonFunction[Float](_params, _fieldName)
  case class VisitParamExtractBool(_params: StringColMagnet, _fieldName: StringColMagnet) extends JsonFunction[Boolean](_params, _fieldName)
  case class VisitParamExtractRaw[T](_params: StringColMagnet, _fieldName: StringColMagnet) extends JsonFunction[T](_params, _fieldName)
  case class VisitParamExtractString(_params: StringColMagnet, _fieldName: StringColMagnet) extends JsonFunction[String](_params, _fieldName)

  def visitParamHas(params: StringColMagnet, fieldName: StringColMagnet) =                  VisitParamHas(params, fieldName)
  def visitParamExtractUInt(params: StringColMagnet, fieldName: StringColMagnet) =          VisitParamExtractUInt(params, fieldName)
  def visitParamExtractInt(params: StringColMagnet, fieldName: StringColMagnet) =           VisitParamExtractInt(params, fieldName)
  def visitParamExtractFloat(params: StringColMagnet, fieldName: StringColMagnet) =         VisitParamExtractFloat(params, fieldName)
  def visitParamExtractBool(params: StringColMagnet, fieldName: StringColMagnet) =          VisitParamExtractBool(params, fieldName)
  def visitParamExtractRaw(params: StringColMagnet, fieldName: StringColMagnet) =           VisitParamExtractRaw(params, fieldName)
  def visitParamExtractString(params: StringColMagnet, fieldName: StringColMagnet) =        VisitParamExtractString(params, fieldName)

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
