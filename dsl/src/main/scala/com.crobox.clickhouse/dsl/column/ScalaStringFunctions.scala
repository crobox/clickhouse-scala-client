package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn

trait ScalaStringFunctions { self: StringFunctions with StringSearchFunctions with LogicalFunctions with Magnets =>

  trait ScalaStringFunctionOps { self: StringSearchOps with StringOps with StringColMagnet[_] =>

    def startsWithAnyOf[S](others: Seq[S])(implicit elMag: S => StringColMagnet[_]): TableColumn[Boolean] =
      like(others.map(other => concat(other, "%")))

    def endsWithAnyOf[S](others: Seq[S])(implicit elMag: S => StringColMagnet[_]): TableColumn[Boolean] =
      like(others.map(other => concat("%", other)))

    def containsAnyOf[S](others: Iterable[S])(implicit elMag: S => StringColMagnet[_]): TableColumn[Boolean] =
      like(others.map(other => concat("%", other, "%")))

    def startsWith(other: StringColMagnet[_]): TableColumn[Boolean] =
      Like(self,Concat(other, "%"))

    def endsWith(other: StringColMagnet[_]): TableColumn[Boolean] =
      Like(self,Concat("%", other))

    def contains(other: StringColMagnet[_]): TableColumn[Boolean] =
      Like(self,Concat("%", other, "%"))

    def like(other: Iterable[TableColumn[String]]): TableColumn[Boolean] =
      other
        .map {
          o => Like(self,o).asInstanceOf[TableColumn[Boolean]]
        }
        .reduce {
          (a, b) => or(a, b)
        }

  }
}