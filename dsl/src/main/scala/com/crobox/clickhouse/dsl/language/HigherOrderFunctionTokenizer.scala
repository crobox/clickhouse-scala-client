package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn, _}

trait HigherOrderFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  /**
   * The lambda, as `x -> body` or `(x,y,...) -> body`.
   *
   * The parameter names are positional and fixed: a lambda parameter is bound to a `RefColumn` carrying one of these,
   * and the body was built against it. `x`, `y` and `z` come first so the rendering of the arities that existed before
   * is unchanged.
   */
  private def tokenizeLambda(lambda: Lambda[_])(implicit ctx: TokenizeContext): String = {
    def render(names: String, body: ExpressionColumn[_]): String = s"$names -> ${tokenizeColumn(body)}, "

    lambda match {
      case Lambda.Of1(f) => render("x", f(RefColumn("x")))
      case Lambda.Of2(f) => render("(x,y)", f(RefColumn("x"), RefColumn("y")))
      case Lambda.Of3(f) => render("(x,y,z)", f(RefColumn("x"), RefColumn("y"), RefColumn("z")))
      case Lambda.Of4(f) =>
        render("(x,y,z,u)", f(RefColumn("x"), RefColumn("y"), RefColumn("z"), RefColumn("u")))
      case Lambda.Of5(f) =>
        render("(x,y,z,u,v)", f(RefColumn("x"), RefColumn("y"), RefColumn("z"), RefColumn("u"), RefColumn("v")))
    }
  }

  // Generic rather than existential: a node such as `ArrayMax[O] extends HigherOrderFunction[O, O]`
  // repeats its type parameter, and an invariant class will not unify that with a pair of wildcards.
  private def tokenizeHOParams[O, R](col: HigherOrderFunction[O, R])(implicit ctx: TokenizeContext): String =
    col.lambda.map(tokenizeLambda).getOrElse("") + tokenizeColumns(col.arrays.map(_.column))

  def tokenizeHigherOrderFunction[O, R](col: HigherOrderFunction[O, R])(implicit ctx: TokenizeContext): String =
    col match {
      case col: ArrayAll               => s"arrayAll(${tokenizeHOParams(col)})"
      case col: ArrayAvg[_]            => s"arrayAvg(${tokenizeHOParams(col)})"
      case col: ArrayCount             => s"arrayCount(${tokenizeHOParams(col)})"
      case col: ArrayCumSum[_]         => s"arrayCumSum(${tokenizeHOParams(col)})"
      case col: ArrayExists            => s"arrayExists(${tokenizeHOParams(col)})"
      case col: ArrayFill[_]           => s"arrayFill(${tokenizeHOParams(col)})"
      case col: ArrayFilter[_]         => s"arrayFilter(${tokenizeHOParams(col)})"
      case col: ArrayFirst[_]          => s"arrayFirst(${tokenizeHOParams(col)})"
      case col: ArrayFirstIndex        => s"arrayFirstIndex(${tokenizeHOParams(col)})"
      case col: ArrayMap[_]            => s"arrayMap(${tokenizeHOParams(col)})"
      case col: ArrayMax[_]            => s"arrayMax(${tokenizeHOParams(col)})"
      case col: ArrayMin[_]            => s"arrayMin(${tokenizeHOParams(col)})"
      case col: ArrayReverseFill[_]    => s"arrayReverseFill(${tokenizeHOParams(col)})"
      case col: ArrayReverseSort[_, _] => s"arrayReverseSort(${tokenizeHOParams(col)})"
      case col: ArrayReverseSplit[_]   => s"arrayReverseSplit(${tokenizeHOParams(col)})"
      case col: ArraySort[_, _]        => s"arraySort(${tokenizeHOParams(col)})"
      case col: ArraySplit[_]          => s"arraySplit(${tokenizeHOParams(col)})"
      case col: ArraySum[_]            => s"arraySum(${tokenizeHOParams(col)})"
    }
}
