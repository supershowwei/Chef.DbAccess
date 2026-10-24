using System;
using System.Linq;
using Chef.DbAccess.Querying;

namespace Chef.DbAccess
{
    /// <summary>
    ///     Chef.DbAccess 查詢（<c>dataAccess.Query()</c>）的擴充方法。
    /// </summary>
    public static class ChefQueryableExtensions
    {
        /// <summary>
        ///     除錯用：不執行查詢，回傳產生的 SQL 與參數值，例如
        ///     <code>
        ///     SELECT [mem_1].[No] AS [Id], ... WHERE [mem_1].[Age] &gt; {=Age_0}
        ///
        ///     -- Parameters
        ///     -- @Age_0 = 30 (Int32)
        ///     </code>
        ///     <c>{=Age_0}</c> 是 Dapper 的字面值替換，執行時會直接寫入數字。
        /// </summary>
        /// <exception cref="InvalidOperationException"><paramref name="source" /> 不是 Chef.DbAccess 的查詢。</exception>
        /// <exception cref="NotSupportedException">查詢中有無法翻譯成 SQL 的運算式。</exception>
        public static string ToQueryString(this IQueryable source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            if (!(source.Provider is ChefQueryProvider provider))
            {
                throw new InvalidOperationException($"ToQueryString only supports queries created by Chef.DbAccess, but the provider is '{source.Provider.GetType()}'.");
            }

            return provider.Compiler.Prepare(source.Expression).ToQueryString();
        }
    }
}
