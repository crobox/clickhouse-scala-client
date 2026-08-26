package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait WindowFunctionTokenizer { self: ClickhouseTokenizerModule =>

  def tokenizeWindowOnlyFunction(col: WindowOnlyFunctionCol[_])(implicit ctx: TokenizeContext): String = {
    // lagInFrame and leadInFrame share a shape: the column, then whichever of offset and default are set. A default
    // without an offset is refused at construction, so the two Options cannot leave a hole in the argument list.
    def inFrame(name: String, column: Column, offset: Option[Column], default: Option[Column]): String =
      s"$name(${(column +: (offset ++ default).toSeq).map(tokenizeColumn).mkString(Tokens.Delimiter)})"

    col match {
      case RowNumber()                          => "row_number()"
      case Rank()                               => "rank()"
      case DenseRank()                          => "dense_rank()"
      case PercentRank()                        => "percent_rank()"
      case Ntile(buckets)                       => s"ntile($buckets)"
      case LagInFrame(column, offset, default)  => inFrame("lagInFrame", column, offset, default)
      case LeadInFrame(column, offset, default) => inFrame("leadInFrame", column, offset, default)
      case NthValue(column, offset)             => s"nth_value(${tokenizeColumn(column)}${Tokens.Delimiter}$offset)"
    }
  }
}
