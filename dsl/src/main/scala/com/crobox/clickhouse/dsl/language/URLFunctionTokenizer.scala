package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait URLFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeURLFunction(col: URLFunction[_])(implicit ctx: TokenizeContext): String = {
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
      case CutURLParameter(_, _)             => "cutURLParameter"
      case unsupported                       => throw new IllegalArgumentException(s"Unsupported command: $unsupported")
    }
    val tail = col match {
      case ExtractURLParameter(_, c2) => ctx.delimiter + tokenizeColumn(c2.column)
      case CutURLParameter(_, c2)     => ctx.delimiter + tokenizeColumn(c2.column)
      case _                          => ""
    }

    s"$command(${tokenizeColumn(col.urlColumn.column)}$tail)"
  }
}
