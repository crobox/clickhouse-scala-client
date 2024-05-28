package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait StringSearchFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeStringSearchFunction(col: StringSearchFunc[_])(implicit ctx: TokenizeContext): String = {
    val command = col match {
      case Position(_, _, true)      => "position"
      case Position(_, _, false)     => "positionCaseInsensitive"
      case PositionUTF8(_, _, true)  => "positionUTF8"
      case PositionUTF8(_, _, false) => "positionCaseInsensitiveUTF8"
      case StrMatch(_, _)            => "match"
      case Extract(_, _)             => "extract"
      case ExtractAll(_, _)          => "extractAll"
      case ILike(_, _)               => "ilike"
      case Like(_, _)                => "like"
      case NotLike(_, _)             => "notLike"
      case ReplaceOne(_, _, _)       => "replaceOne"
      case ReplaceAll(_, _, _)       => "replaceAll"
      case ReplaceRegexpOne(_, _, _) => "replaceRegexpOne"
      case ReplaceRegexpAll(_, _, _) => "replaceRegexpAll"
    }

    val maybeReplaceParam = col match {
      case r: StringSearchReplaceFunc => ctx.fDelim + tokenizeColumn(r.replace.column)
      case _                          => ""
    }
    s"$command(${tokenizeColumn(col.col1.column)}${ctx.fDelim}${tokenizeColumn(col.col2.column)}$maybeReplaceParam)"
  }
}
