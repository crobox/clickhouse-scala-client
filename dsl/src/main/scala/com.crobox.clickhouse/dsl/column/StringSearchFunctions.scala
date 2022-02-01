package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait StringSearchFunctions { self: Magnets =>
  abstract class StringSearchFunc[+V](val col1: StringColMagnet[_], val col2: StringColMagnet[_])
      extends ExpressionColumn[V](col1.column)
  abstract class StringSearchReplaceFunc(col1: StringColMagnet[_],
                                         col2: StringColMagnet[_],
                                         val replace: StringColMagnet[_])
      extends StringSearchFunc[String](col1, col2)

  case class Position(col: StringColMagnet[_], needle: StringColMagnet[_], caseSensitive: Boolean = true)
      extends StringSearchFunc[Long](col, needle)
  case class PositionUTF8(col: StringColMagnet[_], needle: StringColMagnet[_], caseSensitive: Boolean = true)
      extends StringSearchFunc[Long](col, needle)
  case class StrMatch(col: StringColMagnet[_], pattern: StringColMagnet[_])
      extends StringSearchFunc[Boolean](col, pattern)
  case class Extract(col: StringColMagnet[_], pattern: StringColMagnet[_])
      extends StringSearchFunc[String](col, pattern)
  case class ExtractAll(col: StringColMagnet[_], pattern: StringColMagnet[_])
      extends StringSearchFunc[Iterable[String]](col, pattern)
  case class ILike(col: StringColMagnet[_], pattern: StringColMagnet[_]) extends StringSearchFunc[Boolean](col, pattern)
  case class Like(col: StringColMagnet[_], pattern: StringColMagnet[_])  extends StringSearchFunc[Boolean](col, pattern)
  case class NotLike(col: StringColMagnet[_], pattern: StringColMagnet[_])
      extends StringSearchFunc[Boolean](col, pattern)
  case class ReplaceOne(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_])
      extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceAll(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_])
      extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceRegexpOne(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_])
      extends StringSearchReplaceFunc(col, pattern, replacement)
  case class ReplaceRegexpAll(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_])
      extends StringSearchReplaceFunc(col, pattern, replacement)

  def position(col: StringColMagnet[_], needle: StringColMagnet[_]): Position =
    Position(col, needle, caseSensitive = true)

  def position(col: StringColMagnet[_], needle: StringColMagnet[_], caseInsensitive: Boolean): Position =
    if (caseInsensitive) positionCaseInsensitive(col, needle) else position(col, needle)

  def positionCaseInsensitive(col: StringColMagnet[_], needle: StringColMagnet[_]): Position =
    Position(col, needle, caseSensitive = false)

  def positionUTF8(col: StringColMagnet[_], needle: StringColMagnet[_], caseInsensitive: Boolean): PositionUTF8 =
    if (caseInsensitive) positionUTF8CaseInsensitive(col, needle) else positionUTF8(col, needle)

  def positionUTF8(col: StringColMagnet[_], needle: StringColMagnet[_]): PositionUTF8 = PositionUTF8(col, needle)

  def positionUTF8CaseInsensitive(col: StringColMagnet[_], needle: StringColMagnet[_]): PositionUTF8 =
    PositionUTF8(col, needle, caseSensitive = false)
  def strMatch(col: StringColMagnet[_], pattern: StringColMagnet[_]): StrMatch     = StrMatch(col, pattern)
  def extract(col: StringColMagnet[_], pattern: StringColMagnet[_]): Extract       = Extract(col, pattern)
  def extractAll(col: StringColMagnet[_], pattern: StringColMagnet[_]): ExtractAll = ExtractAll(col, pattern)
  def iLike(col: StringColMagnet[_], pattern: StringColMagnet[_]): ILike           = ILike(col, pattern)
  def like(col: StringColMagnet[_], pattern: StringColMagnet[_]): Like             = Like(col, pattern)

  def like(col: StringColMagnet[_], pattern: StringColMagnet[_], caseInsensitive: Boolean): StringSearchFunc[Boolean] =
    if (caseInsensitive) iLike(col, pattern) else like(col, pattern)
  def notLike(col: StringColMagnet[_], pattern: StringColMagnet[_]): NotLike = NotLike(col, pattern)

  def replaceOne(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]): ReplaceOne =
    ReplaceOne(col, pattern, replacement)

  def replaceAll(col: StringColMagnet[_], pattern: StringColMagnet[_], replacement: StringColMagnet[_]): ReplaceAll =
    ReplaceAll(col, pattern, replacement)

  def replaceRegexpOne(col: StringColMagnet[_],
                       pattern: StringColMagnet[_],
                       replacement: StringColMagnet[_]): ReplaceRegexpOne = ReplaceRegexpOne(col, pattern, replacement)

  def replaceRegexpAll(col: StringColMagnet[_],
                       pattern: StringColMagnet[_],
                       replacement: StringColMagnet[_]): ReplaceRegexpAll = ReplaceRegexpAll(col, pattern, replacement)

  trait StringSearchOps { self: StringColMagnet[_] =>

    def position(needle: StringColMagnet[_], caseInsensitive: Boolean): Position =
      if (caseInsensitive) positionCaseInsensitive(needle) else position(needle)
    def position(needle: StringColMagnet[_]): Position                = Position(self, needle, true)
    def positionCaseInsensitive(needle: StringColMagnet[_]): Position = Position(self, needle, false)

    def positionUTF8(needle: StringColMagnet[_], caseInsensitive: Boolean): PositionUTF8 =
      if (caseInsensitive) positionUTF8CaseInsensitive(needle) else positionUTF8(needle)
    def positionUTF8(needle: StringColMagnet[_]): PositionUTF8                = PositionUTF8(self, needle, true)
    def positionUTF8CaseInsensitive(needle: StringColMagnet[_]): PositionUTF8 = PositionUTF8(self, needle, false)
    def strMatch(pattern: StringColMagnet[_]): StrMatch                       = StrMatch(self, pattern)
    def extract(pattern: StringColMagnet[_]): Extract                         = Extract(self, pattern)
    def extractAll(pattern: StringColMagnet[_]): ExtractAll                   = ExtractAll(self, pattern)
    def iLike(pattern: StringColMagnet[_]): ILike                             = ILike(self, pattern)
    def like(pattern: StringColMagnet[_]): Like                               = Like(self, pattern)

    def like(pattern: StringColMagnet[_], caseInsensitive: Boolean): StringSearchFunc[Boolean] =
      if (caseInsensitive) ILike(self, pattern) else Like(self, pattern)
    def notLike(pattern: StringColMagnet[_]): NotLike = NotLike(self, pattern)

    def replaceOne(pattern: StringColMagnet[_], replacement: StringColMagnet[_]): ReplaceOne =
      ReplaceOne(self, pattern, replacement)

    def replaceAll(pattern: StringColMagnet[_], replacement: StringColMagnet[_]): ReplaceAll =
      ReplaceAll(self, pattern, replacement)

    def replaceRegexpOne(pattern: StringColMagnet[_], replacement: StringColMagnet[_]): ReplaceRegexpOne =
      ReplaceRegexpOne(self, pattern, replacement)

    def replaceRegexpAll(pattern: StringColMagnet[_], replacement: StringColMagnet[_]): ReplaceRegexpAll =
      ReplaceRegexpAll(self, pattern, replacement)
  }
}
