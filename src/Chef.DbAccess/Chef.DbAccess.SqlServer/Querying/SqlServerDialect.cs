using System.Collections.Generic;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlGeneration;
using Chef.DbAccess.SqlServer.SqlGeneration;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     SQL Server 方言：<see cref="SqlServerSqlGenerator" /> 與 <see cref="SqlServerTranslators" />。
    /// </summary>
    internal sealed class SqlServerDialect : ISqlDialect
    {
        public static readonly SqlServerDialect Instance = new SqlServerDialect();

        private SqlServerDialect()
        {
        }

        public IReadOnlyList<IMemberTranslator> MemberTranslators => SqlServerTranslators.Members;

        public IReadOnlyList<IMethodCallTranslator> MethodCallTranslators => SqlServerTranslators.MethodCalls;

        public SqlGenerator CreateGenerator()
        {
            return new SqlServerSqlGenerator();
        }
    }
}
