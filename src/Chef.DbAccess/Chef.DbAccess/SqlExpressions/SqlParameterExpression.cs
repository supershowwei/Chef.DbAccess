using System;
using Chef.DbAccess.Metadata;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     SQL 參數，例如 <c>@Name_0</c>。名稱不含前綴符號，由方言決定輸出格式。
    /// </summary>
    public sealed class SqlParameterExpression : SqlExpression
    {
        public SqlParameterExpression(string name, object value, Type type, PropertyMetadata property = null)
            : base(type)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Parameter name can not be null or empty.", nameof(name));

            this.Name = name;
            this.Value = value;
            this.Property = property;
        }

        public string Name { get; }

        public object Value { get; }

        /// <summary>
        ///     參數值要比較或寫入的欄位，方言用它決定資料庫型別（例如 <c>varchar</c> 欄位用非 Unicode 字串傳送）；
        ///     與欄位無關時為 <c>null</c>。
        /// </summary>
        public PropertyMetadata Property { get; }
    }
}
