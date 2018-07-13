package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait URLFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeURLFunction(col: URLFunction[_]): String = {
    val command = col match {
      case Protocol(_)                       => "protocol"
      case Domain(_)                         => "domain"
      case DomainWithoutWWW(_)               => "domainWithoutWWW"
      case TopLevelDomain(_)                 => "topLevelDomain"
      case FirstSignificantSubdomain(_)      => "firstSignificantSubdomain"
      case CutToFirstSignificantSubdomain(_) => "cutToFirstSignificantSubdomain"
      case Path(_)                           => "path"
      case PathFull(_)                       => "pathFull"
      case QueryString(_)                    => "queryString"
      case Fragment(_)                       => "fragment"
      case QueryStringAndFragment(_)         => "queryStringAndFragment"
      case ExtractURLParameter(_, _)         => "extractURLParameter"
      case ExtractURLParameters(_)           => "extractURLParameters"
      case ExtractURLParameterNames(_)       => "extractURLParameterNames"
      case URLHierarchy(_)                   => "URLHierarchy"
      case URLPathHierarchy(_)               => "URLPathHierarchy"
      case DecodeURLComponent(_)             => "decodeURLComponent"
      case CutWWW(_)                         => "cutWWW"
      case CutQueryString(_)                 => "cutQueryString"
      case CutFragment(_)                    => "cutFragment"
      case CutQueryStringAndFragment(_)      => "cutQueryStringAndFragment"
      case CutURLParameter(_)                => "cutURLParameter"
    }
    val tail = col match {
      case ExtractURLParameter(_, c2) => "," + tokenizeColumn(c2.column)
      case _                          => ""
    }

    fast"$command(${tokenizeColumn(col)}$tail)"
  }
}
