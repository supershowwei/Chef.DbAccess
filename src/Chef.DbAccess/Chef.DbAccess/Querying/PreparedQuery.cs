using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     準備好執行的查詢：快取中的 <see cref="CompiledQuery" />，加上這次執行的參數值。
    /// </summary>
    internal sealed class PreparedQuery
    {
        public PreparedQuery(CompiledQuery compiled, IReadOnlyList<SqlParameterExpression> parameters, IReadOnlyDictionary<string, object> parameterValues)
        {
            this.Compiled = compiled;
            this.Parameters = parameters;
            this.ParameterValues = parameterValues;
        }

        public CompiledQuery Compiled { get; }

        public string Sql => this.Compiled.Sql;

        /// <summary>
        ///     這次執行的 SQL 參數（含值），交給方言轉成資料庫的參數。
        /// </summary>
        public IReadOnlyList<SqlParameterExpression> Parameters { get; }

        /// <summary>
        ///     這次執行的查詢參數值，Shaper 取用留在用戶端的值（例如 <c>Tag = tag</c>）時使用。
        /// </summary>
        public IReadOnlyDictionary<string, object> ParameterValues { get; }

        /// <summary>
        ///     除錯用：SQL 加上參數值，格式與 3.x 的特徵化快照相同，例如
        ///     <code>
        ///     SELECT ... WHERE [mem_1].[Age] &gt; {=Age_0}
        ///
        ///     -- Parameters
        ///     -- @Age_0 = 30 (Int32)
        ///     </code>
        /// </summary>
        public string ToQueryString()
        {
            var sb = new StringBuilder(this.Sql);

            if (this.Parameters.Count > 0)
            {
                sb.Append("\n\n-- Parameters");

                foreach (var parameter in this.Parameters)
                {
                    sb.Append("\n-- @").Append(parameter.Name).Append(" = ").Append(FormatValue(parameter.Value));
                }
            }

            return sb.ToString();
        }

        private static string FormatValue(object value)
        {
            switch (value)
            {
                case null: return "NULL";
                case string str: return $"'{str}' (String)";
                case DateTime dateTime: return $"{dateTime.ToString("O", CultureInfo.InvariantCulture)} (DateTime)";
                case DateTimeOffset dateTimeOffset: return $"{dateTimeOffset.ToString("O", CultureInfo.InvariantCulture)} (DateTimeOffset)";
                case IEnumerable enumerable: return $"[{string.Join(", ", enumerable.Cast<object>().Select(FormatValue))}]";
                case IFormattable formattable: return $"{formattable.ToString(null, CultureInfo.InvariantCulture)} ({value.GetType().Name})";
                default: return $"{value} ({value.GetType().Name})";
            }
        }
    }
}
