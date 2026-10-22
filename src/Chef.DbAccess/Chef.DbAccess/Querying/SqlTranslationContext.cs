using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     翻譯一個查詢時共用的狀態：<see cref="PartialEvaluator" /> 算出的參數值，以及 SQL 參數的命名。
    ///     <para>
    ///         SQL 參數沿用 3.x 的命名方式：比較或寫入的欄位屬性名稱加上流水號，例如 <c>Age_0</c>、<c>Age_1</c>；
    ///         和欄位無關時用閉包變數的名稱，都沒有時用 <c>p</c>。
    ///     </para>
    /// </summary>
    internal sealed class SqlTranslationContext
    {
        private readonly IReadOnlyDictionary<string, object> parameterValues;
        private readonly Dictionary<string, int> parameterCounters = new Dictionary<string, int>();

        public SqlTranslationContext(IReadOnlyDictionary<string, object> parameterValues)
        {
            this.parameterValues = parameterValues ?? throw new ArgumentNullException(nameof(parameterValues));
        }

        /// <summary>
        ///     <paramref name="parameter" /> 是否為 <see cref="PartialEvaluator" /> 產生的查詢參數；是的話一併取得這次執行的值。
        /// </summary>
        public bool TryGetParameterValue(ParameterExpression parameter, out object value)
        {
            value = null;

            return parameter.Name != null && this.parameterValues.TryGetValue(parameter.Name, out value);
        }

        public SqlParameterExpression CreateParameter(string baseName, object value, Type type, PropertyMetadata property)
        {
            baseName = property?.PropertyName ?? baseName ?? "p";

            this.parameterCounters.TryGetValue(baseName, out var index);
            this.parameterCounters[baseName] = index + 1;

            return new SqlParameterExpression(string.Concat(baseName, "_", index.ToString()), value, type, property);
        }
    }
}
