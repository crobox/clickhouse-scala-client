package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait StringSearchFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeStringSearchFunction(col: StringSearchFunc[_]): String = {
    val command = col match {
      case Position(_, _, true)      => "position"
      case Position(_, _, false)     => "positionCaseInsensitive"
      case PositionUTF8(_, _, true)  => "positionUTF8"
      case PositionUTF8(_, _, false) => "positionCaseInsensitiveUTF8"
      case StrMatch(_, _)            => "strMatch"
      case Extract(_, _)             => "extract"
      case ExtractAll(_, _)          => "extractAll"
      case Like(_, _)                => "like"
      case NotLike(_, _)             => "notLike"
      case ReplaceOne(_, _, _)       => "replaceOne"
      case ReplaceAll(_, _, _)       => "replaceAll"
      case ReplaceRegexpOne(_, _, _) => "replaceRegexpOne"
      case ReplaceRegexpAll(_, _, _) => "replaceRegexpAll"
    }

    val maybeReplaceParam = col match {
      case r: StringSearchReplaceFunc => "," + tokenizeColumn(r.replace.column)
      case _                          => ""
    }
    fast"$command(${tokenizeColumn(col.col1.column)},${tokenizeColumn(col.col2.column)}$maybeReplaceParam)"
  }
}
