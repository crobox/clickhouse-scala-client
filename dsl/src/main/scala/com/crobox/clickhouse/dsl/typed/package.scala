package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.marshalling.{ColumnDecoder, RowDecoder}

/**
 * The typed entry point: `select` here returns a [[typed.TypedQuery]] carrying a decoder for its own rows, so the result
 * type follows from the select list instead of from a hand-written `JsonReader` whose field names have to match your
 * aliases by hand.
 *
 * Qualify it as `typed.select`. `typed` is a subpackage of `dsl`, so after the usual `import
 * com.crobox.clickhouse.dsl._` no further import is needed:
 *
 * {{{
 * import com.crobox.clickhouse.dsl._
 * import com.crobox.clickhouse.dsl.typed.TypedQuery
 *
 * val q: TypedQuery[(String, Int)] = typed.select(shieldId, col2) from OneTestTable where (col2 > 10)
 * val rows: Seq[(String, Int)]     = executor.executeTyped(q).futureValue.rows
 * }}}
 *
 * Do not wildcard-import this alongside `dsl._`: both define `select`, and the untyped one wins silently, so your query
 * comes back as an `OperationalQuery` and the mismatch only shows up as a confusing type error later. Rename it instead
 * if you want it unqualified -- `import com.crobox.clickhouse.dsl.typed.{select => selectTyped}`.
 *
 * A separate namespace rather than an overload on purpose: adding these arities to `dsl.select` would change the
 * inferred type of every existing call.
 *
 * The arities below are mechanical; twelve is the practical ceiling before a case class is the better answer, and a
 * wider select is still reachable untyped.
 */
package object typed {

  def select[A: ColumnDecoder](c1: TableColumn[A]): TypedQuery[A] =
    TypedQuery(SelectQuery(Seq(c1)), RowDecoder[A])
  def select[A: ColumnDecoder, B: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B]): TypedQuery[(A, B)] =
    TypedQuery(SelectQuery(Seq(c1, c2)), RowDecoder[(A, B)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C]): TypedQuery[(A, B, C)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3)), RowDecoder[(A, B, C)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D]): TypedQuery[(A, B, C, D)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4)), RowDecoder[(A, B, C, D)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E]): TypedQuery[(A, B, C, D, E)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5)), RowDecoder[(A, B, C, D, E)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder, F: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E], c6: TableColumn[F]): TypedQuery[(A, B, C, D, E, F)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5, c6)), RowDecoder[(A, B, C, D, E, F)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder, F: ColumnDecoder, G: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E], c6: TableColumn[F], c7: TableColumn[G]): TypedQuery[(A, B, C, D, E, F, G)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5, c6, c7)), RowDecoder[(A, B, C, D, E, F, G)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder, F: ColumnDecoder, G: ColumnDecoder, H: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E], c6: TableColumn[F], c7: TableColumn[G], c8: TableColumn[H]): TypedQuery[(A, B, C, D, E, F, G, H)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5, c6, c7, c8)), RowDecoder[(A, B, C, D, E, F, G, H)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder, F: ColumnDecoder, G: ColumnDecoder, H: ColumnDecoder, I: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E], c6: TableColumn[F], c7: TableColumn[G], c8: TableColumn[H], c9: TableColumn[I]): TypedQuery[(A, B, C, D, E, F, G, H, I)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5, c6, c7, c8, c9)), RowDecoder[(A, B, C, D, E, F, G, H, I)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder, F: ColumnDecoder, G: ColumnDecoder, H: ColumnDecoder, I: ColumnDecoder, J: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E], c6: TableColumn[F], c7: TableColumn[G], c8: TableColumn[H], c9: TableColumn[I], c10: TableColumn[J]): TypedQuery[(A, B, C, D, E, F, G, H, I, J)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)), RowDecoder[(A, B, C, D, E, F, G, H, I, J)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder, F: ColumnDecoder, G: ColumnDecoder, H: ColumnDecoder, I: ColumnDecoder, J: ColumnDecoder, K: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E], c6: TableColumn[F], c7: TableColumn[G], c8: TableColumn[H], c9: TableColumn[I], c10: TableColumn[J], c11: TableColumn[K]): TypedQuery[(A, B, C, D, E, F, G, H, I, J, K)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)), RowDecoder[(A, B, C, D, E, F, G, H, I, J, K)])
  def select[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder, F: ColumnDecoder, G: ColumnDecoder, H: ColumnDecoder, I: ColumnDecoder, J: ColumnDecoder, K: ColumnDecoder, L: ColumnDecoder](c1: TableColumn[A], c2: TableColumn[B], c3: TableColumn[C], c4: TableColumn[D], c5: TableColumn[E], c6: TableColumn[F], c7: TableColumn[G], c8: TableColumn[H], c9: TableColumn[I], c10: TableColumn[J], c11: TableColumn[K], c12: TableColumn[L]): TypedQuery[(A, B, C, D, E, F, G, H, I, J, K, L)] =
    TypedQuery(SelectQuery(Seq(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)), RowDecoder[(A, B, C, D, E, F, G, H, I, J, K, L)])
}
