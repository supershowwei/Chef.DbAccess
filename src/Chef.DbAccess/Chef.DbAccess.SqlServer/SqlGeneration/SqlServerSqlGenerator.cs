using System.Linq;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.SqlGeneration;

namespace Chef.DbAccess.SqlServer.SqlGeneration
{
    /// <summary>
    ///     SQL Server（T-SQL）方言：<c>[ ]</c> 跳脫、<c>TOP(n)</c>、字串以 <c>+</c> 串接、布林常數為 <c>1</c> / <c>0</c>、預設加上 <c>WITH (NOLOCK)</c>。
    /// </summary>
    internal class SqlServerSqlGenerator : SqlGenerator
    {
        public SqlServerSqlGenerator(bool withNoLock = true)
        {
            this.WithNoLock = withNoLock;
        }

        /// <summary>
        ///     每個資料表都加上 <c>WITH (NOLOCK)</c>，對應 3.x 的 <c>IsDirtyRead</c>。預設開啟，與 3.x 一致。
        /// </summary>
        public bool WithNoLock { get; }

        /// <summary>
        ///     只有 Take 時用 <c>TOP(n)</c>；有 Skip 時由 <see cref="GenerateLimitOffset" /> 輸出 <c>OFFSET ... FETCH</c>。
        /// </summary>
        protected override void GenerateTop(SelectExpression select)
        {
            if (select.Limit == null || select.Offset != null) return;

            this.Sql.Append("TOP(");
            this.Visit(select.Limit);
            this.Sql.Append(") ");
        }

        /// <summary>
        ///     SQL Server 的 <c>OFFSET</c> 必須搭配 <c>ORDER BY</c>，沒有排序時補上 <c>ORDER BY (SELECT 1)</c>。
        /// </summary>
        protected override void GenerateLimitOffset(SelectExpression select)
        {
            if (select.Offset == null) return;

            if (select.Orderings.Count == 0) this.Sql.Append("\nORDER BY (SELECT 1)");

            this.Sql.Append("\nOFFSET ");
            this.Visit(select.Offset);
            this.Sql.Append(" ROWS");

            if (select.Limit != null)
            {
                this.Sql.Append("\nFETCH NEXT ");
                this.Visit(select.Limit);
                this.Sql.Append(" ROWS ONLY");
            }
        }

        protected override void VisitTableExpression(TableExpression table)
        {
            base.VisitTableExpression(table);

            if (this.WithNoLock) this.Sql.Append(" WITH (NOLOCK)");
        }

        protected override string Delimit(string identifier)
        {
            return string.Concat("[", identifier.Replace("]", "]]"), "]");
        }

        protected override string GetOperator(SqlBinaryExpression binary)
        {
            return binary.Operator == SqlBinaryOperator.Add && binary.Type == typeof(string) ? "+" : base.GetOperator(binary);
        }

        /// <summary>
        ///     布林輸出 <c>1</c> / <c>0</c>；含非 ASCII 字元的字串加上 <c>N</c> 前綴，避免在非 Unicode 定序下變成問號。
        /// </summary>
        protected override string FormatConstant(object value)
        {
            switch (value)
            {
                case bool boolean: return boolean ? "1" : "0";
                case string str when str.Any(c => c > 127): return string.Concat("N", base.FormatConstant(str));
                default: return base.FormatConstant(value);
            }
        }
    }
}
