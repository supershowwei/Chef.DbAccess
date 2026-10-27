using System.Collections.Generic;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlGeneration;
using Chef.DbAccess.SqlServer.SqlGeneration;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     SQL Server 方言：<see cref="SqlServerSqlGenerator" /> 與 <see cref="SqlServerTranslators" />。
    ///     <para>
    ///         是否加上 <c>WITH (NOLOCK)</c> 會改變 SQL，所以分成兩個方言，各自搭配一個 <see cref="QueryCompiler" />，快取互不影響。
    ///     </para>
    /// </summary>
    internal sealed class SqlServerDialect : ISqlDialect
    {
        /// <summary>
        ///     預設：每個資料表都加上 <c>WITH (NOLOCK)</c>，與 3.x 的 <c>IsDirtyRead = true</c> 相同。
        /// </summary>
        public static readonly SqlServerDialect Instance = new SqlServerDialect(true);

        /// <summary>
        ///     <c>IsDirtyRead = false</c> 時使用，不加 <c>WITH (NOLOCK)</c>。
        /// </summary>
        public static readonly SqlServerDialect WithoutNoLock = new SqlServerDialect(false);

        private readonly bool withNoLock;

        private SqlServerDialect(bool withNoLock)
        {
            this.withNoLock = withNoLock;
        }

        public IReadOnlyList<IMemberTranslator> MemberTranslators => SqlServerTranslators.Members;

        public IReadOnlyList<IMethodCallTranslator> MethodCallTranslators => SqlServerTranslators.MethodCalls;

        public SqlGenerator CreateGenerator()
        {
            return new SqlServerSqlGenerator(this.withNoLock);
        }
    }
}
