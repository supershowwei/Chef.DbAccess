using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     <c>SELECT</c> 清單中的一項，例如 <c>[m].[No] AS [Id]</c>。
    /// </summary>
    public sealed class ProjectionExpression
    {
        public ProjectionExpression(SqlExpression expression, string alias)
        {
            if (string.IsNullOrEmpty(alias)) throw new ArgumentException("Alias can not be null or empty.", nameof(alias));

            this.Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            this.Alias = alias;
        }

        public SqlExpression Expression { get; }

        public string Alias { get; }
    }
}
