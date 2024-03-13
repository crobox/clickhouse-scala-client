package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn
import scala.language.implicitConversions
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

trait ScalaStringFunctions { self: StringFunctions with StringSearchFunctions with LogicalFunctions with Magnets =>

  trait ScalaStringFunctionOps { self: StringSearchOps with StringOps with StringColMagnet[_] =>

    def startsWithAnyOf[S](others: Seq[S],
                           caseInsensitive: Boolean)(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      if (caseInsensitive) iStartsWithAnyOf(others) else startsWithAnyOf(others)

    def endsWithAnyOf[S](others: Seq[S],
                         caseInsensitive: Boolean)(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      if (caseInsensitive) iEndsWithAnyOf(others) else endsWithAnyOf(others)

    def containsAnyOf[S](others: Iterable[S],
                         caseInsensitive: Boolean)(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      if (caseInsensitive) iContainsAnyOf(others) else containsAnyOf(others)

    def startsWith(other: StringColMagnet[_], caseInsensitive: Boolean): TableColumn[Boolean] =
      if (caseInsensitive) iStartsWith(other) else startsWith(other)

    def endsWith(other: StringColMagnet[_], caseInsensitive: Boolean): TableColumn[Boolean] =
      if (caseInsensitive) iEndsWith(other) else endsWith(other)

    def contains(other: StringColMagnet[_], caseInsensitive: Boolean): TableColumn[Boolean] =
      if (caseInsensitive) iContains(other) else contains(other)

    //
    // Case Sensitive
    //

    def startsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      likeWithAnyOf(others.map(other => concat(ev(other), "%")))

    def endsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      likeWithAnyOf(others.map(other => concat("%", ev(other))))

    def containsAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      likeWithAnyOf(others.map(other => concat("%", ev(other), "%")))

    def startsWith(other: StringColMagnet[_]): TableColumn[Boolean] =
      Like(self, Concat(other, "%"))

    def endsWith(other: StringColMagnet[_]): TableColumn[Boolean] =
      Like(self, Concat("%", other))

    def contains(other: StringColMagnet[_]): TableColumn[Boolean] =
      Like(self, Concat("%", other, "%"))

    def likeWithAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      others.map(o => Like(self, ev(o)).asInstanceOf[TableColumn[Boolean]]).reduce((a, b) => or(a, b))

    //
    // Case Insensitive
    //

    def iStartsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      iLikeWithAnyOf(others.map(other => concat(ev(other), "%")))

    def iEndsWithAnyOf[S](others: Seq[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      iLikeWithAnyOf(others.map(other => concat("%", ev(other))))

    def iContainsAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      iLikeWithAnyOf(others.map(other => concat("%", ev(other), "%")))

    def iStartsWith(other: StringColMagnet[_]): TableColumn[Boolean] =
      ILike(self, Concat(other, "%"))

    def iEndsWith(other: StringColMagnet[_]): TableColumn[Boolean] =
      ILike(self, Concat("%", other))

    def iContains(other: StringColMagnet[_]): TableColumn[Boolean] =
      ILike(self, Concat("%", other, "%"))

    def iLikeWithAnyOf[S](others: Iterable[S])(implicit ev: S => StringColMagnet[_]): TableColumn[Boolean] =
      others.map(o => ILike(self, ev(o)).asInstanceOf[TableColumn[Boolean]]).reduce((a, b) => or(a, b))
  }
}
