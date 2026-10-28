namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     查詢的根（<see cref="ChefQueryable{T}" />）除了元素型別之外會影響 SQL 的資訊。
    ///     <see cref="ExpressionEqualityComparer" /> 比較查詢的根時一併比較，翻譯器依它決定資料表。
    /// </summary>
    internal interface IQueryRoot
    {
        /// <summary>
        ///     指定的資料表名稱，例如 3.x 的 <c>factory.Create&lt;Member&gt;("tbl_member_archive")</c>；未指定時為 <c>null</c>，使用 <c>[Table]</c> 或類別名稱。
        /// </summary>
        string TableName { get; }

        /// <summary>
        ///     連線字串的資料庫伺服器（<c>Data Source</c>）；不知道時為 <c>null</c>。Join 的每個來源都必須在同一台伺服器。
        /// </summary>
        string Server { get; }

        /// <summary>
        ///     連線字串的資料庫（<c>Initial Catalog</c>）；不知道時為 <c>null</c>。Join 的來源在其他資料庫時，資料表名稱要加上資料庫。
        /// </summary>
        string Database { get; }
    }
}
