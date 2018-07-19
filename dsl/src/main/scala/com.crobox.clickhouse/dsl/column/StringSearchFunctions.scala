package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait StringSearchFunctions { self: Magnets =>
  abstract class StringSearchFunc[V](val col1: StringColMagnet[_], val col2: StringColMagnet[_]) extends ExpressionColumn[V](col1.column)
  abstract class StringSearchReplaceFunc(col1: StringColMagnet[_], col2: StringColMagnet[_], val replace: StringColMagnet[_]) extends StringSearchFunc[String](col1, col2)
  
  case class Position(col: StringColMagnet[_], needle: StringColMagnet[_], caseSensitive: Boolean = true) extends StringSearchFunc[Long](col,needle)
  case class PositionUTF8(col: StringColMagnet[_], needle: StringColMagnet[_], caseSensitive: Boolean = true) extends StringSearchFunc[Long](col,needle)
  case class StrMatch(col: StringColMagnet[_], pattern: StringColMagnet[_]) extends StringSearchFunc[Boolean](col, pattern)
  case class Extract(col: StringColMagnet[_], pattern: StringColMagnet[_]) extends StringSearchFunc[String](col, pattern)
  case class ExtractAll(col: StringColMagnet[_], pattern: StringColMagnet[_]) extends StringSearchFunc[String](col, pattern)
  case class Like(col: StringColMagnet[_], pattern: StringColMagnet[_]) extends StringSearchFunc[Boolean](col, pattern)
  case class NotLike(col: StringColMagnet[_], pattern: StringColMagnet[_]) extends StringSearchFunc[Boolean](col, pattern)
  case class ReplaceOne(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceAll(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceRegexpOne(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceRegexpAll(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) extends StringSearchReplaceFunc(col, pattern, replacement)
  
  def position(col: StringColMagnet[_], needle: StringColMagnet[_]) = Position(col, needle)
  def positionCaseInsensitive(col: StringColMagnet[_], needle: StringColMagnet[_]) = Position(col, needle, false)
  def positionUTF8(col: StringColMagnet[_], needle: StringColMagnet[_]) = PositionUTF8(col, needle)
  def positionUTF8CaseInsensitive(col: StringColMagnet[_], needle: StringColMagnet[_]) = PositionUTF8(col, needle, false)
  def strMatch(col: StringColMagnet[_], pattern: StringColMagnet[_]) = StrMatch(col, pattern)
  def extract(col: StringColMagnet[_], pattern: StringColMagnet[_]) = Extract(col, pattern)
  def extractAll(col: StringColMagnet[_], pattern: StringColMagnet[_]) = ExtractAll(col, pattern)
  def like(col: StringColMagnet[_], pattern: StringColMagnet[_]) = Like(col, pattern)
  def notLike(col: StringColMagnet[_], pattern: StringColMagnet[_]) = NotLike(col, pattern)
  def replaceOne(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) = ReplaceOne(col, pattern, replacement)
  def replaceAll(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) = ReplaceAll(col, pattern, replacement)
  def replaceRegexpOne(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) = ReplaceRegexpOne(col, pattern, replacement)
  def replaceRegexpAll(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]) = ReplaceRegexpAll(col, pattern, replacement)
  
  
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
