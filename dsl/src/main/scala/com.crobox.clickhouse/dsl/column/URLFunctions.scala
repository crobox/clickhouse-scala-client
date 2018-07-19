package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait URLFunctions { self: Magnets =>
  sealed abstract class URLFunction[V](column: StringColMagnet[_]) extends ExpressionColumn[V](column.column)
  abstract class URLStrFunction(col: StringColMagnet[_]) extends URLFunction[String](col)
  abstract class URLArrFunction(col: StringColMagnet[_]) extends URLFunction[Seq[String]](col)

  case class Protocol(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class Domain(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class DomainWithoutWWW(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class TopLevelDomain(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class FirstSignificantSubdomain(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class CutToFirstSignificantSubdomain(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class Path(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class PathFull(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class QueryString(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class Fragment(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class QueryStringAndFragment(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class ExtractURLParameter(col: StringColMagnet[_], param: StringColMagnet[_]) extends URLStrFunction(col)
  case class ExtractURLParameters(col: StringColMagnet[_]) extends URLArrFunction(col)
  case class ExtractURLParameterNames(col: StringColMagnet[_]) extends URLArrFunction(col)
  case class URLHierarchy(col: StringColMagnet[_]) extends URLArrFunction(col)
  case class URLPathHierarchy(col: StringColMagnet[_]) extends URLArrFunction(col)
  case class DecodeURLComponent(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class CutWWW(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class CutQueryString(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class CutFragment(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class CutQueryStringAndFragment(col: StringColMagnet[_]) extends URLStrFunction(col)
  case class CutURLParameter(col: StringColMagnet[_]) extends URLStrFunction(col)

  def protocol(col: StringColMagnet[_]) = Protocol(col)
  def domain(col: StringColMagnet[_]) = Domain(col)
  def domainWithoutWWW(col: StringColMagnet[_]) = DomainWithoutWWW(col)
  def topLevelDomain(col: StringColMagnet[_]) = TopLevelDomain(col)
  def firstSignificantSubdomain(col: StringColMagnet[_]) = FirstSignificantSubdomain(col)
  def cutToFirstSignificantSubdomain(col: StringColMagnet[_]) = CutToFirstSignificantSubdomain(col)
  def path(col: StringColMagnet[_]) = Path(col)
  def pathFull(col: StringColMagnet[_]) = PathFull(col)
  def queryString(col: StringColMagnet[_]) = QueryString(col)
  def fragment(col: StringColMagnet[_]) = Fragment(col)
  def queryStringAndFragment(col: StringColMagnet[_]) = QueryStringAndFragment(col)
  def extractURLParameter(col: StringColMagnet[_], param: StringColMagnet[_]) = ExtractURLParameter(col, param)
  def extractURLParameters(col: StringColMagnet[_]) = ExtractURLParameters(col)
  def extractURLParameterNames(col: StringColMagnet[_]) = ExtractURLParameterNames(col)
  def uRLHierarchy(col: StringColMagnet[_]) = URLHierarchy(col)
  def uRLPathHierarchy(col: StringColMagnet[_]) = URLPathHierarchy(col)
  def decodeURLComponent(col: StringColMagnet[_]) = DecodeURLComponent(col)

  def cutWWW(col: StringColMagnet[_]) = CutWWW(col)
  def cutQueryString(col: StringColMagnet[_]) = CutQueryString(col)
  def cutFragment(col: StringColMagnet[_]) = CutFragment(col)
  def cutQueryStringAndFragment(col: StringColMagnet[_]) = CutQueryStringAndFragment(col)
  def cutURLParameter(col: StringColMagnet[_]) = CutURLParameter(col)



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
