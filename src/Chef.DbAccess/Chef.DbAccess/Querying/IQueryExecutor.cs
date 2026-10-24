using System.Collections.Generic;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     實際連到資料庫執行 <see cref="PreparedQuery" />，由各資料庫的方言提供。
    ///     <c>First</c>、<c>Single</c>、<c>Count</c> 等結果的語意由 <see cref="ChefQueryProvider" /> 處理，這裡只負責讀取資料列。
    /// </summary>
    internal interface IQueryExecutor
    {
        /// <summary>
        ///     執行查詢，以 <see cref="CompiledQuery.GetShaper{T}" /> 把每一列組成 <typeparamref name="T" />。
        /// </summary>
        IEnumerable<T> Query<T>(PreparedQuery query);

        /// <summary>
        ///     執行查詢，回傳是否有任何資料列（<c>Any</c> / <c>All</c>）。
        /// </summary>
        bool HasRows(PreparedQuery query);
    }
}
