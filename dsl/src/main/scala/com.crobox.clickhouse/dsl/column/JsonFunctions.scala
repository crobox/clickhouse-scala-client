package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait JsonFunctions { self: Magnets =>
  abstract class JsonFunction[T](params: StringColMagnet) extends ExpressionColumn[T](params.column)

  case class VisitParamHas(params: StringColMagnet, fieldName: StringColMagnet)   extends JsonFunction[Boolean](params)
  case class VisitParamExtractUInt(params: StringColMagnet, fieldName: StringColMagnet) extends JsonFunction[Long](params)
  case class VisitParamExtractInt(params: StringColMagnet, fieldName: StringColMagnet) extends JsonFunction[Long](params)
  case class VisitParamExtractFloat(params: StringColMagnet, fieldName: StringColMagnet) extends JsonFunction[Float](params)
  case class VisitParamExtractBool(params: StringColMagnet, fieldName: StringColMagnet) extends JsonFunction[Boolean](params)
  case class VisitParamExtractRaw[T](params: StringColMagnet, fieldName: StringColMagnet) extends JsonFunction[T](params)
  case class VisitParamExtractString(params: StringColMagnet, fieldName: StringColMagnet) extends JsonFunction[String](params)

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
