using System;
using System.Linq;
using System.Linq.Expressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     查詢快取的鍵：參數化後運算式的形狀，加上哪些參數的值是 <c>null</c>。
    ///     <para>
    ///         參數是 <c>null</c> 時翻譯結果不同（<c>x.Name == name</c> 要輸出 <c>IS NULL</c> 而不是 <c>= @name</c>），
    ///         所以同樣的形狀會依參數是否為 <c>null</c> 分成不同的快取項目。
    ///     </para>
    /// </summary>
    internal sealed class QueryCacheKey : IEquatable<QueryCacheKey>
    {
        private readonly Expression expression;
        private readonly string[] nullParameterNames;
        private readonly int hashCode;

        public QueryCacheKey(ParameterizedQuery query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            this.expression = query.Expression;
            this.nullParameterNames = query.ParameterValues.Where(p => p.Value == null).Select(p => p.Key).OrderBy(n => n, StringComparer.Ordinal).ToArray();

            unchecked
            {
                this.hashCode = this.nullParameterNames.Aggregate(
                    ExpressionEqualityComparer.Instance.GetHashCode(this.expression),
                    (hash, name) => (hash * 31) + StringComparer.Ordinal.GetHashCode(name));
            }
        }

        public bool Equals(QueryCacheKey other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.hashCode == other.hashCode
                   && this.nullParameterNames.SequenceEqual(other.nullParameterNames, StringComparer.Ordinal)
                   && ExpressionEqualityComparer.Instance.Equals(this.expression, other.expression);
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as QueryCacheKey);
        }

        public override int GetHashCode()
        {
            return this.hashCode;
        }
    }
}
