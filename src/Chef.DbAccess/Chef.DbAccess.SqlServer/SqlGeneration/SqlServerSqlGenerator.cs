using System;
using System.Collections.Generic;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.SqlGeneration;

namespace Chef.DbAccess.SqlServer.SqlGeneration
{
    /// <summary>
    ///     SQL Server（T-SQL）方言：<c>[ ]</c> 跳脫、<c>TOP(n)</c>、字串以 <c>+</c> 串接、布林常數為 <c>1</c> / <c>0</c>、預設加上 <c>WITH (NOLOCK)</c>、
    ///     數值、布林與列舉參數使用 Dapper 的 <c>{=name}</c> 字面值替換。
    /// </summary>
    internal class SqlServerSqlGenerator : SqlGenerator
    {
        private static readonly HashSet<Type> LiteralTypes = new HashSet<Type>
                                                             {
                                                                 typeof(bool),
                                                                 typeof(byte),
                                                                 typeof(sbyte),
                                                                 typeof(short),
                                                                 typeof(ushort),
                                                                 typeof(int),
                                                                 typeof(uint),
                                                                 typeof(long),
                                                                 typeof(ulong),
                                                                 typeof(float),
                                                                 typeof(double),
                                                                 typeof(decimal)
                                                             };

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

        /// <summary>
        ///     數值、布林與列舉參數使用 Dapper 的字面值替換語法 <c>{=name}</c>，執行時由 Dapper 把值直接寫進 SQL
        ///     （列舉寫成底層的數字），讓不同的值各自產生執行計畫，減少參數探查（parameter sniffing）造成的問題。
        ///     <para>3.x 只對數值與布林這樣做，列舉用 <c>@name</c>。值為 <c>null</c> 時仍用 <c>@name</c>，避免寫成 <c>= null</c>。</para>
        /// </summary>
        protected override string GetParameterPlaceholder(SqlParameterExpression parameter)
        {
            var valueType = parameter.Value?.GetType();

            return valueType != null && (valueType.IsEnum || LiteralTypes.Contains(valueType))
                       ? string.Concat("{=", parameter.Name, "}")
                       : base.GetParameterPlaceholder(parameter);
        }

        /// <summary>
        ///     回傳 <see cref="long" /> 的 <c>COUNT</c>（<c>LongCount</c>）輸出 <c>COUNT_BIG</c>；SQL Server 的 <c>COUNT</c> 回傳 <c>int</c>，超過約 21 億筆會溢位。
        /// </summary>
        protected override void VisitFunction(SqlFunctionExpression function)
        {
            if (function.Name == "COUNT" && function.Type == typeof(long)) function = new SqlFunctionExpression("COUNT_BIG", function.Arguments, function.Type);

            base.VisitFunction(function);
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
        ///     布林輸出 <c>1</c> / <c>0</c>。
        ///     <para>
        ///         字串常數刻意不加 <c>N</c> 前綴：<c>N'...'</c> 與 <c>varchar</c> 欄位比較時，SQL Server 會把欄位隱含轉換成 <c>nvarchar</c>，
        ///         可能用不到索引。使用者的字串值應該走參數，依欄位型別對應（見 <see cref="SqlServerParameterMapper" />）。
        ///     </para>
        /// </summary>
        protected override string FormatConstant(object value)
        {
            return value is bool boolean ? boolean ? "1" : "0" : base.FormatConstant(value);
        }
    }
}
