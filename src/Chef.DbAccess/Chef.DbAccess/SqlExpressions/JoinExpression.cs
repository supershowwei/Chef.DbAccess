using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     <c>INNER JOIN</c> 或 <c>LEFT JOIN</c> 一個來源，例如 <c>INNER JOIN [tbl_department] [dep_2] ON [mem_1].[DepartmentId] = [dep_2].[Id]</c>。
    ///     <para>
    ///         放在 <see cref="SelectExpression.Tables" /> 第一個來源之後；別名沿用被 Join 的來源，
    ///         欄位（<see cref="ColumnExpression.Table" />）參考的是 <see cref="Table" />，不是 Join 本身。
    ///     </para>
    /// </summary>
    public sealed class JoinExpression : TableExpressionBase
    {
        public JoinExpression(JoinType joinType, TableExpressionBase table, SqlExpression predicate)
            : base((table ?? throw new ArgumentNullException(nameof(table))).Alias)
        {
            if (table is JoinExpression || table is CrossJoinExpression) throw new ArgumentException("Can not join a join expression.", nameof(table));

            this.JoinType = joinType;
            this.Table = table;
            this.Predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
        }

        public JoinType JoinType { get; }

        public TableExpressionBase Table { get; }

        /// <summary>
        ///     <c>ON</c> 條件。
        /// </summary>
        public SqlExpression Predicate { get; }
    }
}
