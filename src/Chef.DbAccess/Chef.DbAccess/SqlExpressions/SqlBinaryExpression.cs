using System;

namespace Chef.DbAccess.SqlExpressions
{
    public enum SqlBinaryOperator
    {
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
        AndAlso,
        OrElse,
        Add,
        Subtract,
        Multiply,
        Divide,
        Modulo,
        BitwiseAnd,
        BitwiseOr,
        BitwiseXor,
        Like
    }

    /// <summary>
    ///     二元運算，例如 <c>[m].[Age] &gt; @Age_0</c>、<c>[m].[Name] LIKE @Name_0 + '%'</c>。
    /// </summary>
    public sealed class SqlBinaryExpression : SqlExpression
    {
        public SqlBinaryExpression(SqlBinaryOperator @operator, SqlExpression left, SqlExpression right, Type type)
            : base(type)
        {
            this.Operator = @operator;
            this.Left = left ?? throw new ArgumentNullException(nameof(left));
            this.Right = right ?? throw new ArgumentNullException(nameof(right));
        }

        public SqlBinaryOperator Operator { get; }

        public SqlExpression Left { get; }

        public SqlExpression Right { get; }
    }
}
