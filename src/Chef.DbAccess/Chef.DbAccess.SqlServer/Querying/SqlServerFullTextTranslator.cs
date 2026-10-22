using System.Collections.Generic;
using System.Reflection;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.Syntax;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     SQL Server 全文檢索：<c>x.Name.Includes("Johnny")</c>（<see cref="StringExtension.Includes" />）→ <c>CONTAINS([Name], @Name_0)</c>，與 3.x 相同。
    ///     <para>值直接交給 <c>CONTAINS</c>，使用者可以寫全文檢索的搜尋條件（例如 <c>"\"John*\""</c>），不做任何跳脫。</para>
    /// </summary>
    internal sealed class SqlServerFullTextTranslator : IMethodCallTranslator
    {
        public static readonly SqlServerFullTextTranslator Instance = new SqlServerFullTextTranslator();

        private SqlServerFullTextTranslator()
        {
        }

        public SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (method.DeclaringType != typeof(StringExtension) || method.Name != nameof(StringExtension.Includes) || arguments.Count != 2) return null;

            return new SqlFunctionExpression("CONTAINS", new[] { arguments[0], arguments[1] }, typeof(bool));
        }
    }
}
