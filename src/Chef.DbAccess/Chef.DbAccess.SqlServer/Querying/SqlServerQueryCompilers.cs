using Chef.DbAccess.Querying;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     所有 SQL Server 查詢共用的 <see cref="QueryCompiler" />（含快取），依是否加上 <c>WITH (NOLOCK)</c> 分成兩個。
    /// </summary>
    internal static class SqlServerQueryCompilers
    {
        public static readonly QueryCompiler WithNoLock = new QueryCompiler(SqlServerDialect.Instance);

        public static readonly QueryCompiler WithoutNoLock = new QueryCompiler(SqlServerDialect.WithoutNoLock);
    }
}
