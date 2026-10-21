using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     <c>ORDER BY</c> 中的一項，例如 <c>[m].[Age] DESC</c>。
    /// </summary>
    public sealed class OrderingExpression
    {
        public OrderingExpression(SqlExpression expression, bool isAscending)
        {
            this.Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            this.IsAscending = isAscending;
        }

        public SqlExpression Expression { get; }

        public bool IsAscending { get; }
    }
}
