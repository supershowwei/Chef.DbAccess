using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     SQL 參數，例如 <c>@Name_0</c>。名稱不含前綴符號，由方言決定輸出格式。
    /// </summary>
    public sealed class SqlParameterExpression : SqlExpression
    {
        public SqlParameterExpression(string name, object value, Type type)
            : base(type)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Parameter name can not be null or empty.", nameof(name));

            this.Name = name;
            this.Value = value;
        }

        public string Name { get; }

        public object Value { get; }
    }
}
