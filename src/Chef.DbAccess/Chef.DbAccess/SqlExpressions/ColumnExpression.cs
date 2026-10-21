using System;
using Chef.DbAccess.Metadata;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     資料表的欄位，例如 <c>[m].[No]</c>。
    /// </summary>
    public sealed class ColumnExpression : SqlExpression
    {
        public ColumnExpression(TableExpressionBase table, string name, Type type, PropertyMetadata property = null)
            : base(type)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Column name can not be null or empty.", nameof(name));

            this.Table = table ?? throw new ArgumentNullException(nameof(table));
            this.Name = name;
            this.Property = property;
        }

        public TableExpressionBase Table { get; }

        public string Name { get; }

        /// <summary>
        ///     欄位對應的屬性，用來取得 <c>[Column(TypeName)]</c>、<c>[StringLength]</c> 等型別資訊；非實體欄位時為 <c>null</c>。
        /// </summary>
        public PropertyMetadata Property { get; }
    }
}
