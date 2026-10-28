using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Chef.DbAccess.Metadata
{
    /// <summary>
    ///     以 <c>[ForeignKey]</c> 設定的參考導覽屬性（多對一），例如 <c>Member.Department</c> 以 <c>Member.DepartmentId</c> 對應到 <c>Department</c> 的鍵。
    ///     <para>
    ///         與 EF 相同，<c>[ForeignKey]</c> 可以標在導覽屬性上（值是外鍵屬性名稱，複合鍵以逗號分隔），或標在外鍵屬性上（值是導覽屬性名稱）。
    ///         沒有 <c>[ForeignKey]</c> 的導覽屬性不會依名稱推斷。
    ///     </para>
    /// </summary>
    public sealed class NavigationMetadata
    {
        private readonly Lazy<IReadOnlyList<PropertyMetadata>> principalKeys;

        internal NavigationMetadata(PropertyInfo property, IReadOnlyList<PropertyMetadata> foreignKeys)
        {
            this.Property = property;
            this.ForeignKeys = foreignKeys;
            this.principalKeys = new Lazy<IReadOnlyList<PropertyMetadata>>(this.ResolvePrincipalKeys);
        }

        public PropertyInfo Property { get; }

        public string PropertyName => this.Property.Name;

        public Type TargetType => this.Property.PropertyType;

        /// <summary>
        ///     宣告導覽屬性的實體上的外鍵欄位，順序與 <see cref="PrincipalKeys" /> 對應。
        /// </summary>
        public IReadOnlyList<PropertyMetadata> ForeignKeys { get; }

        /// <summary>
        ///     被參考的實體。用到時才解析，避免 <c>Member.Manager</c> 這種參考自己的型別在建立 metadata 時互相遞迴。
        /// </summary>
        public EntityMetadata TargetEntity => EntityMetadata.For(this.TargetType);

        /// <summary>
        ///     被參考的鍵：<see cref="TargetEntity" /> 的 <c>[Key]</c>；沒有 <c>[Key]</c> 時是名為 <c>Id</c> 的欄位。數量必須與 <see cref="ForeignKeys" /> 相同。
        /// </summary>
        public IReadOnlyList<PropertyMetadata> PrincipalKeys => this.principalKeys.Value;

        public override string ToString()
        {
            return $"{this.PropertyName} -> {this.TargetType.Name} ({string.Join(", ", this.ForeignKeys.Select(k => k.PropertyName))})";
        }

        private IReadOnlyList<PropertyMetadata> ResolvePrincipalKeys()
        {
            var target = this.TargetEntity;
            var keys = target.Keys.Count > 0 ? target.Keys : new[] { target.FindColumn("Id") }.Where(k => k != null).ToList();

            if (keys.Count != this.ForeignKeys.Count)
            {
                throw new InvalidOperationException(
                    $"Navigation '{this.Property.DeclaringType?.Name}.{this.PropertyName}' has {this.ForeignKeys.Count} foreign key(s), " +
                    $"but '{this.TargetType.Name}' has {keys.Count} key(s). Mark the keys of '{this.TargetType.Name}' with [Key].");
            }

            return keys;
        }
    }
}
