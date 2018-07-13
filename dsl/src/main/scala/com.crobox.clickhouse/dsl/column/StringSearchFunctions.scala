package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait StringSearchFunctions { self: Magnets =>
  abstract class StringSearchFunc[V](val col1: StringColMagnet, val col2: StringColMagnet) extends ExpressionColumn[V](col1.column)
  abstract class StringSearchReplaceFunc(col1: StringColMagnet, col2: StringColMagnet, val replace: StringColMagnet) extends StringSearchFunc[String](col1, col2)
  
  case class Position(col: StringColMagnet, needle: StringColMagnet, caseSensitive: Boolean = true) extends StringSearchFunc[Long](col,needle)
  case class PositionUTF8(col: StringColMagnet, needle: StringColMagnet, caseSensitive: Boolean = true) extends StringSearchFunc[Long](col,needle)
  case class StrMatch(col: StringColMagnet, pattern: StringColMagnet) extends StringSearchFunc[Boolean](col, pattern)
  case class Extract(col: StringColMagnet, pattern: StringColMagnet) extends StringSearchFunc[String](col, pattern)
  case class ExtractAll(col: StringColMagnet, pattern: StringColMagnet) extends StringSearchFunc[String](col, pattern)
  case class Like(col: StringColMagnet, pattern: StringColMagnet) extends StringSearchFunc[Boolean](col, pattern)
  case class NotLike(col: StringColMagnet, pattern: StringColMagnet) extends StringSearchFunc[Boolean](col, pattern)
  case class ReplaceOne(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceAll(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceRegexpOne(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceRegexpAll(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) extends StringSearchReplaceFunc(col, pattern, replacement)
  
  def position(col: StringColMagnet, needle: StringColMagnet) = Position(col, needle)
  def positionCaseInsensitive(col: StringColMagnet, needle: StringColMagnet) = Position(col, needle, false)
  def positionUTF8(col: StringColMagnet, needle: StringColMagnet) = PositionUTF8(col, needle)
  def positionUTF8CaseInsensitive(col: StringColMagnet, needle: StringColMagnet) = PositionUTF8(col, needle, false)
  def strMatch(col: StringColMagnet, pattern: StringColMagnet) = StrMatch(col, pattern)
  def extract(col: StringColMagnet, pattern: StringColMagnet) = Extract(col, pattern)
  def extractAll(col: StringColMagnet, pattern: StringColMagnet) = ExtractAll(col, pattern)
  def like(col: StringColMagnet, pattern: StringColMagnet) = Like(col, pattern)
  def notLike(col: StringColMagnet, pattern: StringColMagnet) = NotLike(col, pattern)
  def replaceOne(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) = ReplaceOne(col, pattern, replacement)
  def replaceAll(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) = ReplaceAll(col, pattern, replacement)
  def replaceRegexpOne(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) = ReplaceRegexpOne(col, pattern, replacement)
  def replaceRegexpAll(col: StringColMagnet, pattern: StringColMagnet, replacement: StringColMagnet) = ReplaceRegexpAll(col, pattern, replacement)
  
  
/*
position(col, needle)
positionUTF8(col, needle)
match(col, pattern)
extract(col, pattern)
extractAll(col, pattern)
like(col, pattern), haystack LIKE pattern operator
notLike(col, pattern), haystack NOT LIKE pattern operator


replaceOne(col, pattern, replacement)
replaceAll(col, pattern, replacement)
replaceRegexpOne(col, pattern, replacement)
replaceRegexpAll(col, pattern, replacement)
 */
}
