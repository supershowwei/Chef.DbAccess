using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     一對多 <c>Include</c> 的 identity resolution：<c>LEFT JOIN</c> 之後同一筆父資料會出現在多個資料列，
    ///     每列由 Shaper 組出「父資料 + 集合中 0 或 1 筆子資料」（見 <see cref="CreateCollection{T}" />），這裡依鍵合併成同一個物件。
    ///     <list type="bullet">
    ///         <item>父資料依第一次出現的順序回傳，所以 <c>OrderBy</c> 的順序不變。</item>
    ///         <item>子資料也依鍵去除重複：有兩個集合 <c>Include</c> 時，資料列是兩個集合的交叉組合。</item>
    ///         <item>要讀完所有資料列才能合併，所以結果會先全部讀進記憶體。</item>
    ///     </list>
    /// </summary>
    internal sealed class IdentityResolver
    {
        private readonly IReadOnlyList<PropertyInfo> keys;
        private readonly IReadOnlyList<(PropertyInfo Collection, IReadOnlyList<PropertyInfo> Keys)> collections;

        /// <param name="keys">父資料的鍵。</param>
        /// <param name="collections">集合屬性，以及子資料的鍵。</param>
        public IdentityResolver(IReadOnlyList<PropertyInfo> keys, IReadOnlyList<(PropertyInfo Collection, IReadOnlyList<PropertyInfo> Keys)> collections)
        {
            this.keys = keys;
            this.collections = collections;
        }

        /// <summary>
        ///     Shaper 為每列建立的集合：子資料是 <c>null</c>（<c>LEFT JOIN</c> 沒有對應的資料）時是空的集合。
        /// </summary>
        public static List<T> CreateCollection<T>(T item)
            where T : class
        {
            var collection = new List<T>();

            if (item != null) collection.Add(item);

            return collection;
        }

        public List<T> Resolve<T>(IEnumerable<T> rows)
        {
            var results = new List<T>();
            var resolved = new Dictionary<object[], (T Root, HashSet<object[]>[] ChildKeys)>(KeyComparer.Instance);

            foreach (var row in rows)
            {
                var key = GetKey(row, this.keys);

                if (!resolved.TryGetValue(key, out var entry))
                {
                    entry = (row, this.collections.Select(_ => new HashSet<object[]>(KeyComparer.Instance)).ToArray());

                    resolved.Add(key, entry);
                    results.Add(row);
                }

                for (var i = 0; i < this.collections.Count; i++)
                {
                    var (collection, childKeys) = this.collections[i];
                    var target = (IList)collection.GetValue(entry.Root);

                    // 第一次出現的父資料本身就帶著這一列的子資料，只記錄鍵，之後的重複才會被去除。
                    foreach (var child in (IEnumerable)collection.GetValue(row))
                    {
                        if (entry.ChildKeys[i].Add(GetKey(child, childKeys)) && !ReferenceEquals(row, entry.Root)) target.Add(child);
                    }
                }
            }

            return results;
        }

        private static object[] GetKey(object entity, IReadOnlyList<PropertyInfo> keys)
        {
            return keys.Select(k => k.GetValue(entity)).ToArray();
        }

        private sealed class KeyComparer : IEqualityComparer<object[]>
        {
            public static readonly KeyComparer Instance = new KeyComparer();

            public bool Equals(object[] x, object[] y)
            {
                return x.SequenceEqual(y);
            }

            public int GetHashCode(object[] key)
            {
                return key.Aggregate(17, (hash, value) => unchecked(hash * 31 + (value?.GetHashCode() ?? 0)));
            }
        }
    }
}
