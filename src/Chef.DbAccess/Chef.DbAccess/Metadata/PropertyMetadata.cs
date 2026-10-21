using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace Chef.DbAccess.Metadata
{
    /// <summary>
    ///     一個對應到資料表欄位的屬性。
    /// </summary>
    public sealed class PropertyMetadata
    {
        internal PropertyMetadata(PropertyInfo property)
        {
            var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
            var databaseGenerated = property.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption;

            this.Property = property;
            this.ColumnName = string.IsNullOrEmpty(columnAttribute?.Name) ? property.Name : columnAttribute.Name;
            this.ColumnTypeName = string.IsNullOrEmpty(columnAttribute?.TypeName) ? null : columnAttribute.TypeName;
            this.MaxLength = property.GetCustomAttribute<StringLengthAttribute>()?.MaximumLength ?? property.GetCustomAttribute<MaxLengthAttribute>()?.Length;
            this.IsRequired = Attribute.IsDefined(property, typeof(RequiredAttribute));
            this.IsKey = Attribute.IsDefined(property, typeof(KeyAttribute));
            this.IsIdentity = databaseGenerated == DatabaseGeneratedOption.Identity;
            this.IsComputed = databaseGenerated == DatabaseGeneratedOption.Computed;
        }

        public PropertyInfo Property { get; }

        public string PropertyName => this.Property.Name;

        public Type ClrType => this.Property.PropertyType;

        /// <summary>
        ///     <c>[Column(Name)]</c>，未指定時為屬性名稱。
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        ///     <c>[Column(TypeName = ...)]</c>，未指定時為 <c>null</c>。
        /// </summary>
        public string ColumnTypeName { get; }

        /// <summary>
        ///     <c>[StringLength]</c> 或 <c>[MaxLength]</c>，未指定時為 <c>null</c>。
        /// </summary>
        public int? MaxLength { get; }

        public bool IsRequired { get; }

        public bool IsKey { get; }

        /// <summary>
        ///     <c>[DatabaseGenerated(DatabaseGeneratedOption.Identity)]</c>
        /// </summary>
        public bool IsIdentity { get; }

        /// <summary>
        ///     <c>[DatabaseGenerated(DatabaseGeneratedOption.Computed)]</c>
        /// </summary>
        public bool IsComputed { get; }

        public override string ToString()
        {
            return $"{this.PropertyName} -> [{this.ColumnName}]";
        }
    }
}
