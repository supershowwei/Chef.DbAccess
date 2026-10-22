using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     一次執行的查詢參數值。多包一層是為了記住集合參數去掉 <c>null</c> 後的元素：
    ///     <c>ids.Contains(x.Id)</c> 的每個 SQL 參數都要取「第 i 個非 null 元素」，每個參數各自列舉一次集合會是 O(n²)。
    /// </summary>
    internal sealed class QueryParameterValues : IReadOnlyDictionary<string, object>
    {
        private readonly IReadOnlyDictionary<string, object> values;
        private readonly Dictionary<string, IReadOnlyList<object>> nonNullElements = new Dictionary<string, IReadOnlyList<object>>();

        public QueryParameterValues(IReadOnlyDictionary<string, object> values)
        {
            this.values = values;
        }

        public int Count => this.values.Count;

        public IEnumerable<string> Keys => this.values.Keys;

        public IEnumerable<object> Values => this.values.Values;

        public object this[string key] => this.values[key];

        /// <summary>
        ///     集合參數去掉 <c>null</c> 後的元素；集合本身是 <c>null</c> 時為空。<paramref name="values" /> 是 <see cref="QueryParameterValues" /> 時只列舉一次。
        /// </summary>
        public static IReadOnlyList<object> GetNonNullElements(IReadOnlyDictionary<string, object> values, string name)
        {
            if (!(values is QueryParameterValues parameterValues)) return Materialize(values[name]);

            if (!parameterValues.nonNullElements.TryGetValue(name, out var elements))
            {
                parameterValues.nonNullElements[name] = elements = Materialize(parameterValues.values[name]);
            }

            return elements;
        }

        public bool ContainsKey(string key)
        {
            return this.values.ContainsKey(key);
        }

        public bool TryGetValue(string key, out object value)
        {
            return this.values.TryGetValue(key, out value);
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return this.values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private static IReadOnlyList<object> Materialize(object collection)
        {
            return collection == null ? new object[0] : ((IEnumerable)collection).Cast<object>().Where(e => e != null).ToArray();
        }
    }
}
