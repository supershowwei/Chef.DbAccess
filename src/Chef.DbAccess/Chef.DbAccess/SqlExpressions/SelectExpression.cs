using System;
using System.Collections.Generic;
using System.Linq;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     一個 <c>SELECT</c> 查詢：
    ///     <c>SELECT [DISTINCT] {Projections} FROM {Tables} WHERE {Predicate} ORDER BY {Orderings} {Offset/Limit}</c>。
    ///     <para>
    ///         這裡只描述結構；Skip / Take / Where 疊加的順序規則（例如 Take 之後再 Where 需要子查詢）由翻譯器處理。
    ///         <see cref="Offset" /> 與 <see cref="Limit" /> 要怎麼輸出（TOP 或 OFFSET FETCH）由方言決定。
    ///     </para>
    /// </summary>
    public sealed class SelectExpression
    {
        public SelectExpression(
            IEnumerable<ProjectionExpression> projections,
            IEnumerable<TableExpressionBase> tables,
            SqlExpression predicate = null,
            IEnumerable<OrderingExpression> orderings = null,
            SqlExpression offset = null,
            SqlExpression limit = null,
            bool isDistinct = false)
        {
            this.Projections = projections?.ToArray() ?? throw new ArgumentNullException(nameof(projections));
            this.Tables = tables?.ToArray() ?? throw new ArgumentNullException(nameof(tables));
            this.Predicate = predicate;
            this.Orderings = orderings?.ToArray() ?? Array.Empty<OrderingExpression>();
            this.Offset = offset;
            this.Limit = limit;
            this.IsDistinct = isDistinct;

            if (this.Projections.Count == 0) throw new ArgumentException("Must be at least one projection.", nameof(projections));
            if (this.Projections.Any(p => p == null)) throw new ArgumentException("Projections can not contain null.", nameof(projections));
            if (this.Tables.Count == 0) throw new ArgumentException("Must be at least one table.", nameof(tables));
            if (this.Tables.Any(t => t == null)) throw new ArgumentException("Tables can not contain null.", nameof(tables));
            if (this.Orderings.Any(o => o == null)) throw new ArgumentException("Orderings can not contain null.", nameof(orderings));
        }

        public IReadOnlyList<ProjectionExpression> Projections { get; }

        public IReadOnlyList<TableExpressionBase> Tables { get; }

        public SqlExpression Predicate { get; }

        public IReadOnlyList<OrderingExpression> Orderings { get; }

        /// <summary>
        ///     要跳過的筆數（Skip），未指定時為 <c>null</c>。
        /// </summary>
        public SqlExpression Offset { get; }

        /// <summary>
        ///     要取得的筆數（Take），未指定時為 <c>null</c>。
        /// </summary>
        public SqlExpression Limit { get; }

        public bool IsDistinct { get; }
    }
}
