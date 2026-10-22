using System;
using System.Collections;
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
    ///         <item>集合參數的元素個數與是否有 <c>null</c>：<c>ids.Contains(x.Id)</c> 輸出 <c>IN</c> 時每個元素是一個參數。</item>
    ///     </list>
    /// </summary>
    internal sealed class QueryCacheKey : IEquatable<QueryCacheKey>
    {
        private readonly Expression expression;
        private readonly string[] valueSignatures;
        private readonly int hashCode;

        public QueryCacheKey(ParameterizedQuery query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            this.expression = query.Expression;
            this.valueSignatures = query.ParameterValues.Select(p => (p.Key, Signature: GetValueSignature(p.Value)))
                .Where(p => p.Signature != null)
                .Select(p => string.Concat(p.Key, "=", p.Signature))
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToArray();

            unchecked
            {
                this.hashCode = this.valueSignatures.Aggregate(
                    ExpressionEqualityComparer.Instance.GetHashCode(this.expression),
                    (hash, signature) => (hash * 31) + StringComparer.Ordinal.GetHashCode(signature));
            }
        }

        public bool Equals(QueryCacheKey other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.hashCode == other.hashCode
                   && this.valueSignatures.SequenceEqual(other.valueSignatures, StringComparer.Ordinal)
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

        /// <summary>
        ///     參數值中會改變翻譯結果的狀態；不會改變時回傳 <c>null</c>。
        /// </summary>
        private static string GetValueSignature(object value)
        {
            switch (value)
            {
                case null: return "null";
                case string str: return LikePattern.NeedsEscape(str) ? "like" : null;

                // 二進位資料是單一個值，不是集合。
                case byte[] _: return null;

                case IEnumerable collection:
                    var count = 0;
                    var hasNull = false;

                    foreach (var element in collection)
                    {
                        if (element == null)
                        {
                            hasNull = true;
                        }
                        else
                        {
                            count++;
                        }
                    }

                    return hasNull ? string.Concat(count.ToString(), ",null") : count.ToString();

                default: return null;
            }
        }
    }
}
