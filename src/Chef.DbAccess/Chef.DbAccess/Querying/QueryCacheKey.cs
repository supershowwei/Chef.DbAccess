using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     查詢快取的鍵：參數化後運算式的形狀，加上會改變翻譯結果的參數值狀態。
    ///     <list type="bullet">
    ///         <item>值是 <c>null</c> 的參數：<c>x.Name == name</c> 要輸出 <c>IS NULL</c> 而不是 <c>= @name</c>。</item>
    ///         <item>
    ///             字串值需要 LIKE 跳脫的參數：<c>x.Name.Contains(keyword)</c> 只在值裡有 <c>%</c>、<c>_</c> 等字元時才加上 <c>ESCAPE</c>（見 <see cref="LikePattern" />）。
    ///             這裡不知道參數會不會用在 LIKE，所以所有字串參數都檢查，頂多多分出一個快取項目。
    ///         </item>
    ///     </list>
    /// </summary>
    internal sealed class QueryCacheKey : IEquatable<QueryCacheKey>
    {
        private readonly Expression expression;
        private readonly string[] nullParameterNames;
        private readonly string[] likeEscapeParameterNames;
        private readonly int hashCode;

        public QueryCacheKey(ParameterizedQuery query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            this.expression = query.Expression;
            this.nullParameterNames = SortedNames(query.ParameterValues.Where(p => p.Value == null));
            this.likeEscapeParameterNames = SortedNames(query.ParameterValues.Where(p => p.Value is string value && LikePattern.NeedsEscape(value)));

            unchecked
            {
                // 兩組名稱之間加上分隔值，避免同一個名稱出現在不同組時得到相同的雜湊值。
                var hash = this.nullParameterNames.Aggregate(ExpressionEqualityComparer.Instance.GetHashCode(this.expression), Combine);

                this.hashCode = this.likeEscapeParameterNames.Aggregate((hash * 31) + 1, Combine);
            }
        }

        public bool Equals(QueryCacheKey other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.hashCode == other.hashCode
                   && this.nullParameterNames.SequenceEqual(other.nullParameterNames, StringComparer.Ordinal)
                   && this.likeEscapeParameterNames.SequenceEqual(other.likeEscapeParameterNames, StringComparer.Ordinal)
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

        private static string[] SortedNames(IEnumerable<KeyValuePair<string, object>> parameters)
        {
            return parameters.Select(p => p.Key).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        }

        private static int Combine(int hash, string name)
        {
            unchecked
            {
                return (hash * 31) + StringComparer.Ordinal.GetHashCode(name);
            }
        }
    }
}
