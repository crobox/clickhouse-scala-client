package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait URLFunctions { self: Magnets =>
  sealed trait URLFunction
  abstract class URLStrFunction(col: StringColMagnet) extends ExpressionColumn[String](col.column) with URLFunction
  abstract class URLArrFunction(col: StringColMagnet) extends ExpressionColumn[String](col.column) with URLFunction

  case class Protocol(col: StringColMagnet) extends URLStrFunction(col)
  case class Domain(col: StringColMagnet) extends URLStrFunction(col)
  case class DomainWithoutWWW(col: StringColMagnet) extends URLStrFunction(col)
  case class TopLevelDomain(col: StringColMagnet) extends URLStrFunction(col)
  case class FirstSignificantSubdomain(col: StringColMagnet) extends URLStrFunction(col)
  case class CutToFirstSignificantSubdomain(col: StringColMagnet) extends URLStrFunction(col)
  case class Path(col: StringColMagnet) extends URLStrFunction(col)
  case class PathFull(col: StringColMagnet) extends URLStrFunction(col)
  case class QueryString(col: StringColMagnet) extends URLStrFunction(col)
  case class Fragment(col: StringColMagnet) extends URLStrFunction(col)
  case class QueryStringAndFragment(col: StringColMagnet) extends URLStrFunction(col)
  case class ExtractURLParameter(col: StringColMagnet, param: StringColMagnet) extends URLStrFunction(col)
  case class ExtractURLParameters(col: StringColMagnet) extends URLArrFunction(col)
  case class ExtractURLParameterNames(col: StringColMagnet) extends URLArrFunction(col)
  case class URLHierarchy(col: StringColMagnet) extends URLArrFunction(col)
  case class URLPathHierarchy(col: StringColMagnet) extends URLArrFunction(col)
  case class DecodeURLComponent(col: StringColMagnet) extends URLStrFunction(col)

  case class CutWWW(col: StringColMagnet) extends URLStrFunction(col)
  case class CutQueryString(col: StringColMagnet) extends URLStrFunction(col)
  case class CutFragment(col: StringColMagnet) extends URLStrFunction(col)
  case class CutQueryStringAndFragment(col: StringColMagnet) extends URLStrFunction(col)
  case class CutURLParameter(col: StringColMagnet) extends URLStrFunction(col)

  def protocol(col: StringColMagnet) = Protocol(col)
  def domain(col: StringColMagnet) = Domain(col)
  def domainWithoutWWW(col: StringColMagnet) = DomainWithoutWWW(col)
  def topLevelDomain(col: StringColMagnet) = TopLevelDomain(col)
  def firstSignificantSubdomain(col: StringColMagnet) = FirstSignificantSubdomain(col)
  def cutToFirstSignificantSubdomain(col: StringColMagnet) = CutToFirstSignificantSubdomain(col)
  def path(col: StringColMagnet) = Path(col)
  def pathFull(col: StringColMagnet) = PathFull(col)
  def queryString(col: StringColMagnet) = QueryString(col)
  def fragment(col: StringColMagnet) = Fragment(col)
  def queryStringAndFragment(col: StringColMagnet) = QueryStringAndFragment(col)
  def extractURLParameter(col: StringColMagnet, param: StringColMagnet) = ExtractURLParameter(col, param)
  def extractURLParameters(col: StringColMagnet) = ExtractURLParameters(col)
  def extractURLParameterNames(col: StringColMagnet) = ExtractURLParameterNames(col)
  def uRLHierarchy(col: StringColMagnet) = URLHierarchy(col)
  def uRLPathHierarchy(col: StringColMagnet) = URLPathHierarchy(col)
  def decodeURLComponent(col: StringColMagnet) = DecodeURLComponent(col)

  def cutWWW(col: StringColMagnet) = CutWWW(col)
  def cutQueryString(col: StringColMagnet) = CutQueryString(col)
  def cutFragment(col: StringColMagnet) = CutFragment(col)
  def cutQueryStringAndFragment(col: StringColMagnet) = CutQueryStringAndFragment(col)
  def cutURLParameter(col: StringColMagnet) = CutURLParameter(col)



/*
Functions that extract part of a URL
protocol
domain
domainWithoutWWW
topLevelDomain
firstSignificantSubdomain
cutToFirstSignificantSubdomain
path
pathFull
queryString
fragment
queryStringAndFragment
extractURLParameter
extractURLParameters
extractURLParameterNames
URLHierarchy
URLPathHierarchy
decodeURLComponent

Functions that remove part of a URL.
cutWWW
cutQueryString
cutFragment
cutQueryStringAndFragment
cutURLParameter
 */
}
