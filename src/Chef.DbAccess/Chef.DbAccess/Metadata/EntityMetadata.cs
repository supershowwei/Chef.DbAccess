using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace Chef.DbAccess.Metadata
{
    /// <summary>
    ///     類別與資料表的對應資訊，由 DataAnnotations 解析，每個型別只解析一次。
    /// </summary>
    public sealed class EntityMetadata
    {
        private static readonly ConcurrentDictionary<Type, EntityMetadata> Cache = new ConcurrentDictionary<Type, EntityMetadata>();

        private static readonly HashSet<Type> ScalarTypes = new HashSet<Type>
                                                            {
                                                                typeof(bool),
                                                                typeof(byte),
                                                                typeof(sbyte),
                                                                typeof(short),
                                                                typeof(ushort),
                                                                typeof(int),
                                                                typeof(uint),
                                                                typeof(long),
                                                                typeof(ulong),
                                                                typeof(float),
                                                                typeof(double),
                                                                typeof(decimal),
                                                                typeof(DateTime),
                                                                typeof(DateTimeOffset),
                                                                typeof(TimeSpan),
                                                                typeof(Guid),
                                                                typeof(byte[]),
                                                                typeof(string)
                                                            };

        private readonly Dictionary<string, PropertyMetadata> columnMap;

        private readonly Dictionary<string, NavigationMetadata> navigationMap;

        private EntityMetadata(Type clrType)
        {
            this.ClrType = clrType;

            var tableAttribute = clrType.GetCustomAttribute<TableAttribute>();

            this.TableName = string.IsNullOrEmpty(tableAttribute?.Name) ? clrType.Name : tableAttribute.Name;
            this.Schema = string.IsNullOrEmpty(tableAttribute?.Schema) ? null : tableAttribute.Schema;

            var properties = clrType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0)
                .Where(p => !Attribute.IsDefined(p, typeof(NotMappedAttribute)))
                .ToList();

            this.Columns = properties.Where(p => IsScalar(p.PropertyType)).Select(p => new PropertyMetadata(p)).ToList();

            this.Keys = this.Columns.Where(c => c.IsKey).ToList();

            // 以 new 遮蔽基底屬性時會出現同名屬性，取衍生類別宣告的那一個（GetProperties 會先列出）。
            this.columnMap = this.Columns.GroupBy(c => c.PropertyName).ToDictionary(g => g.Key, g => g.First());

            this.Navigations = this.CreateNavigations(properties);
            this.navigationMap = this.Navigations.GroupBy(n => n.PropertyName).ToDictionary(g => g.Key, g => g.First());
        }

        public Type ClrType { get; }

        /// <summary>
        ///     <c>[Table(Name)]</c>，未指定時為類別名稱。
        /// </summary>
        public string TableName { get; }

        /// <summary>
        ///     <c>[Table(Schema = ...)]</c>，未指定時為 <c>null</c>。
        /// </summary>
        public string Schema { get; }

        /// <summary>
        ///     所有對應到欄位的屬性。排除 <c>[NotMapped]</c>、導覽屬性（類別、集合）與索引子。
        /// </summary>
        public IReadOnlyList<PropertyMetadata> Columns { get; }

        /// <summary>
        ///     標註 <c>[Key]</c> 的欄位。
        /// </summary>
        public IReadOnlyList<PropertyMetadata> Keys { get; }

        /// <summary>
        ///     以 <c>[ForeignKey]</c> 設定的參考導覽屬性，見 <see cref="NavigationMetadata" />。
        /// </summary>
        public IReadOnlyList<NavigationMetadata> Navigations { get; }

        public static EntityMetadata For<T>()
        {
            return For(typeof(T));
        }

        public static EntityMetadata For(Type clrType)
        {
            if (clrType == null) throw new ArgumentNullException(nameof(clrType));

            return Cache.GetOrAdd(clrType, type => new EntityMetadata(type));
        }

        /// <summary>
        ///     依屬性名稱找出欄位，找不到（含 <c>[NotMapped]</c> 與導覽屬性）時回傳 <c>null</c>。
        /// </summary>
        public PropertyMetadata FindColumn(string propertyName)
        {
            return this.columnMap.TryGetValue(propertyName, out var column) ? column : null;
        }

        /// <summary>
        ///     依屬性名稱找出導覽屬性，找不到時回傳 <c>null</c>。
        /// </summary>
        public NavigationMetadata FindNavigation(string propertyName)
        {
            return this.navigationMap.TryGetValue(propertyName, out var navigation) ? navigation : null;
        }

        /// <summary>
        ///     參考導覽屬性是型別為類別、不是欄位也不是集合的屬性。<c>[ForeignKey]</c> 指到不存在或不是欄位的屬性時丟出 <see cref="InvalidOperationException" />。
        /// </summary>
        private List<NavigationMetadata> CreateNavigations(IReadOnlyList<PropertyInfo> properties)
        {
            var references = properties.Where(p => !IsScalar(p.PropertyType) && p.PropertyType.IsClass && !typeof(IEnumerable).IsAssignableFrom(p.PropertyType))
                .GroupBy(p => p.Name)
                .ToDictionary(g => g.Key, g => g.First());

            // 標在外鍵屬性上的 [ForeignKey("Department")]
            var keysByNavigation = this.Columns.Select(c => (Column: c, Name: c.Property.GetCustomAttribute<ForeignKeyAttribute>()?.Name))
                .Where(x => x.Name != null)
                .GroupBy(x => x.Name, x => x.Column)
                .ToDictionary(g => g.Key, g => g.ToList());

            var unknown = keysByNavigation.Keys.FirstOrDefault(n => !references.ContainsKey(n));

            if (unknown != null) throw new InvalidOperationException($"[ForeignKey(\"{unknown}\")] on '{this.ClrType.Name}' must refer to a reference navigation property.");

            var navigations = new List<NavigationMetadata>();

            foreach (var property in references.Values)
            {
                // 標在導覽屬性上的 [ForeignKey("DepartmentId")] 優先。
                var names = property.GetCustomAttribute<ForeignKeyAttribute>()?.Name.Split(',').Select(n => n.Trim()).ToList();
                var foreignKeys = names?.Select(n => this.FindColumn(n) ?? throw new InvalidOperationException(
                                                          $"[ForeignKey] on '{this.ClrType.Name}.{property.Name}' refers to '{n}', which is not a mapped property."))
                                      .ToList();

                if (foreignKeys == null && !keysByNavigation.TryGetValue(property.Name, out foreignKeys)) continue;

                navigations.Add(new NavigationMetadata(property, foreignKeys));
            }

            return navigations;
        }

        private static bool IsScalar(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;

            return type.IsEnum || ScalarTypes.Contains(type);
        }
    }
}
