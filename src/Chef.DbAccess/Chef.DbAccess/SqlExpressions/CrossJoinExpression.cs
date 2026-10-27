using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     <c>CROSS JOIN</c> 一個來源，例如 <c>CROSS JOIN [tbl_department] [dep_2]</c>。
    ///     <para>
    ///         不用 <c>FROM a, b</c>：逗號的優先順序比 <c>JOIN</c> 低，<c>FROM a, b INNER JOIN c ON a.x = c.y</c> 的 <c>ON</c> 看不到 <c>a</c>。
    ///         也不放進 <see cref="JoinType" />：那是 3.x Join API 的公開列舉，舊 API 不認得 Cross。
    ///     </para>
    ///     <para>與 <see cref="JoinExpression" /> 相同，別名沿用被 Join 的來源，欄位參考的是 <see cref="Table" />。</para>
    /// </summary>
    public sealed class CrossJoinExpression : TableExpressionBase
    {
        public CrossJoinExpression(TableExpressionBase table)
            : base((table ?? throw new ArgumentNullException(nameof(table))).Alias)
        {
            if (table is JoinExpression || table is CrossJoinExpression) throw new ArgumentException("Can not join a join expression.", nameof(table));

            this.Table = table;
        }

        public TableExpressionBase Table { get; }
    }
}
