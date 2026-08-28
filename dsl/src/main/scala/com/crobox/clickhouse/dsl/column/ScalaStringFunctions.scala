package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn
import scala.language.implicitConversions
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

trait ScalaStringFunctions { self: StringFunctions with StringSearchFunctions with LogicalFunctions with Magnets =>

  trait ScalaStringFunctionOps { self: StringSearchOps with StringOps with StringColMagnet[_] =>

    def startsWithAnyOf[S](others: Seq[S], caseInsensitive: Boolean)(implicit
        ev: S => StringColMagnet[_]
    ): ExpressionColumn[Boolean] =
      if (caseInsensitive) iStartsWithAnyOf(others) else startsWithAnyOf(others)

    def endsWithAnyOf[S](others: Seq[S], caseInsensitive: Boolean)(implicit
        ev: S => StringColMagnet[_]
    ): ExpressionColumn[Boolean] =
      if (caseInsensitive) iEndsWithAnyOf(others) else endsWithAnyOf(others)

    def containsAnyOf[S](others: Iterable[S], caseInsensitive: Boolean)(implicit
        ev: S => StringColMagnet[_]
    ): ExpressionColumn[Boolean] =
      if (caseInsensitive) iContainsAnyOf(others) else containsAnyOf(others)

    def startsWith(other: StringColMagnet[_], caseInsensitive: Boolean): ExpressionColumn[Boolean] =
      if (caseInsensitive) iStartsWith(other) else startsWith(other)

    def endsWith(other: StringColMagnet[_], caseInsensitive: Boolean): ExpressionColumn[Boolean] =
      if (caseInsensitive) iEndsWith(other) else endsWith(other)

    def contains(other: StringColMagnet[_], caseInsensitive: Boolean): ExpressionColumn[Boolean] =
      if (caseInsensitive) iContains(other) else contains(other)

    //
    // Case Sensitive
    //

    def startsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      likeWithAnyOf(others.map(other => concat(ev(other), "%")))

    def endsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      likeWithAnyOf(others.map(other => concat("%", ev(other))))

    def containsAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      likeWithAnyOf(others.map(other => concat("%", ev(other), "%")))

    def startsWith(other: StringColMagnet[_]): ExpressionColumn[Boolean] =
      Like(self, Concat(other, "%"))

    def endsWith(other: StringColMagnet[_]): ExpressionColumn[Boolean] =
      Like(self, Concat("%", other))

    def contains(other: StringColMagnet[_]): ExpressionColumn[Boolean] =
      Like(self, Concat("%", other, "%"))

    def likeWithAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      others.map(o => Like(self, ev(o))).reduce[ExpressionColumn[Boolean]]((a, b) => or(a, b))

    //
    // Case Insensitive
    //

    def iStartsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      iLikeWithAnyOf(others.map(other => concat(ev(other), "%")))

    def iEndsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      iLikeWithAnyOf(others.map(other => concat("%", ev(other))))

    def iContainsAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      iLikeWithAnyOf(others.map(other => concat("%", ev(other), "%")))

    def iStartsWith(other: StringColMagnet[_]): ExpressionColumn[Boolean] =
      ILike(self, Concat(other, "%"))

    def iEndsWith(other: StringColMagnet[_]): ExpressionColumn[Boolean] =
      ILike(self, Concat("%", other))

    def iContains(other: StringColMagnet[_]): ExpressionColumn[Boolean] =
      ILike(self, Concat("%", other, "%"))

    def iLikeWithAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): ExpressionColumn[Boolean] =
      others.map(o => ILike(self, ev(o))).reduce[ExpressionColumn[Boolean]]((a, b) => or(a, b))
  }
}
