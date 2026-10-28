using System;
using Chef.DbAccess.Metadata;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     資料表，例如 <c>[dbo].[tbl_member] [mem_1]</c>；在其他資料庫時是 <c>[Member].[dbo].[tbl_member] [mem_1]</c>。
    /// </summary>
    public sealed class TableExpression : TableExpressionBase
    {
        public TableExpression(string name, string schema, string alias, EntityMetadata entity = null, string database = null)
            : base(alias)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Table name can not be null or empty.", nameof(name));

            this.Name = name;
            this.Schema = string.IsNullOrEmpty(schema) ? null : schema;
            this.Entity = entity;
            this.Database = string.IsNullOrEmpty(database) ? null : database;
        }

        public string Name { get; }

        /// <summary>
        ///     資料表在查詢連線以外的資料庫（同一台伺服器）時指定；未指定時為 <c>null</c>。
        /// </summary>
        public string Database { get; }

        /// <summary>
        ///     未指定時為 <c>null</c>，由方言決定是否輸出預設 schema。
        /// </summary>
        public string Schema { get; }

        /// <summary>
        ///     資料表對應的實體；非實體資料表時為 <c>null</c>。
        /// </summary>
        public EntityMetadata Entity { get; }

        public static TableExpression For(EntityMetadata entity, string alias)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            return new TableExpression(entity.TableName, entity.Schema, alias, entity);
        }

        public ColumnExpression Column(PropertyMetadata property)
        {
            if (property == null) throw new ArgumentNullException(nameof(property));

            return new ColumnExpression(this, property.ColumnName, property.ClrType, property);
        }
    }
}
