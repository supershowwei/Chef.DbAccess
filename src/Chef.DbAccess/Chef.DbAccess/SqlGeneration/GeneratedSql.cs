using System.Collections.Generic;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.SqlGeneration
{
    /// <summary>
    ///     <see cref="SqlGenerator" /> 產生的 SQL 字串與其使用到的參數（依出現順序，不重複）。
    /// </summary>
    public sealed class GeneratedSql
    {
        internal GeneratedSql(string sql, IReadOnlyList<SqlParameterExpression> parameters)
        {
            this.Sql = sql;
            this.Parameters = parameters;
        }

        public string Sql { get; }

        public IReadOnlyList<SqlParameterExpression> Parameters { get; }

        public override string ToString()
        {
            return this.Sql;
        }
    }
}
