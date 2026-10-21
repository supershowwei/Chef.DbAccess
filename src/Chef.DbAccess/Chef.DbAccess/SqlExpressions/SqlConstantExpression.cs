using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     直接寫在 SQL 裡的常數，例如 <c>1 = 0</c>、<c>TOP(10)</c>。使用者提供的值應該用 <see cref="SqlParameterExpression" />。
    /// </summary>
    public sealed class SqlConstantExpression : SqlExpression
    {
        public SqlConstantExpression(object value, Type type)
            : base(type)
        {
            this.Value = value;
        }

        public object Value { get; }
    }
}
