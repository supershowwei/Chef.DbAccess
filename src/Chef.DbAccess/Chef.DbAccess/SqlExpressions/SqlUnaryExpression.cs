using System;

namespace Chef.DbAccess.SqlExpressions
{
    public enum SqlUnaryOperator
    {
        Not,
        Negate,
        IsNull,
        IsNotNull
    }

    /// <summary>
    ///     一元運算，例如 <c>NOT (...)</c>、<c>-[m].[Age]</c>、<c>[m].[Phone] IS NULL</c>。
    /// </summary>
    public sealed class SqlUnaryExpression : SqlExpression
    {
        public SqlUnaryExpression(SqlUnaryOperator @operator, SqlExpression operand, Type type)
            : base(type)
        {
            this.Operator = @operator;
            this.Operand = operand ?? throw new ArgumentNullException(nameof(operand));
        }

        public SqlUnaryOperator Operator { get; }

        public SqlExpression Operand { get; }
    }
}
