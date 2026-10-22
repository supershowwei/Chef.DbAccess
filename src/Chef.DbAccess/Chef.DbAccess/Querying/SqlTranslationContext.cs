using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     翻譯一個查詢時共用的狀態：<see cref="PartialEvaluator" /> 算出的參數值、SQL 參數的命名，以及每個 SQL 參數的值從哪裡來。
    ///     <para>
    ///         SQL 參數沿用 3.x 的命名方式：比較或寫入的欄位屬性名稱加上流水號，例如 <c>Age_0</c>、<c>Age_1</c>；
    ///         和欄位無關時用閉包變數的名稱，都沒有時用 <c>p</c>。
    ///     </para>
    ///     <para>
    ///         翻譯結果會被快取，下次執行時閉包變數的值不同，所以每個 SQL 參數都記錄一個取值方式（<see cref="ParameterBindings" />），
    ///         依新的查詢參數值重新算出 SQL 參數的值：閉包變數取新的值、集合取新集合的第 i 個元素、LIKE 再跳脫一次，
    ///         寫在 lambda 裡的常數則固定不變（常數是運算式形狀的一部分，不同的常數不會共用快取）。
    ///     </para>
    /// </summary>
    internal sealed class SqlTranslationContext
    {
        private readonly IReadOnlyDictionary<string, object> parameterValues;
        private readonly Dictionary<string, int> parameterCounters = new Dictionary<string, int>();
        private readonly Dictionary<string, Func<IReadOnlyDictionary<string, object>, object>> parameterBindings =
            new Dictionary<string, Func<IReadOnlyDictionary<string, object>, object>>();

        public SqlTranslationContext(IReadOnlyDictionary<string, object> parameterValues)
        {
            this.parameterValues = parameterValues ?? throw new ArgumentNullException(nameof(parameterValues));
        }

        /// <summary>
        ///     SQL 參數名稱對應到「依查詢參數值算出 SQL 參數值」的方式。不保留這次執行的參數值，可以放進快取。
        /// </summary>
        public IReadOnlyDictionary<string, Func<IReadOnlyDictionary<string, object>, object>> ParameterBindings => this.parameterBindings;

        /// <summary>
        ///     <see cref="PartialEvaluator" /> 產生的參數名稱是 <c>__age_0</c>，取出中間的 <c>age</c>。
        /// </summary>
        public static string GetBaseName(string parameterName)
        {
            var name = parameterName.StartsWith("__", StringComparison.Ordinal) ? parameterName.Substring(2) : parameterName;
            var underscore = name.LastIndexOf('_');

            return underscore > 0 && name.Skip(underscore + 1).All(char.IsDigit) ? name.Substring(0, underscore) : name;
        }

        /// <summary>
        ///     <paramref name="parameter" /> 是否為 <see cref="PartialEvaluator" /> 產生的查詢參數；是的話一併取得這次執行的值。
        /// </summary>
        public bool TryGetParameterValue(ParameterExpression parameter, out object value)
        {
            value = null;

            return parameter.Name != null && this.parameterValues.TryGetValue(parameter.Name, out value);
        }

        /// <summary>
        ///     值固定不變的 SQL 參數，例如寫在 lambda 裡的常數、布林欄位條件的 <c>true</c> / <c>false</c>。
        /// </summary>
        public SqlParameterExpression CreateParameter(string baseName, object value, Type type, PropertyMetadata property)
        {
            return this.CreateParameter(baseName, value, type, property, _ => value);
        }

        /// <summary>
        ///     值來自查詢參數（閉包變數、<c>Skip</c> / <c>Take</c> 的數字）的 SQL 參數，下次執行時取新的值。
        /// </summary>
        public SqlParameterExpression CreateParameter(ParameterExpression queryParameter, Type type, PropertyMetadata property)
        {
            var name = queryParameter.Name;

            return this.CreateParameter(GetBaseName(name), this.parameterValues[name], type, property, values => values[name]);
        }

        /// <summary>
        ///     值依 <paramref name="binding" /> 從查詢參數算出的 SQL 參數，例如集合參數的第 i 個元素。
        /// </summary>
        public SqlParameterExpression CreateParameter(
            string baseName,
            object value,
            Type type,
            PropertyMetadata property,
            Func<IReadOnlyDictionary<string, object>, object> binding)
        {
            baseName = property?.PropertyName ?? baseName ?? "p";

            this.parameterCounters.TryGetValue(baseName, out var index);
            this.parameterCounters[baseName] = index + 1;

            var name = string.Concat(baseName, "_", index.ToString());

            this.parameterBindings[name] = binding ?? throw new ArgumentNullException(nameof(binding));

            return new SqlParameterExpression(name, value, type, property);
        }

        /// <summary>
        ///     把 SQL 參數的值換成 <paramref name="transform" /> 的結果（例如 LIKE 跳脫），之後重新取值時也一樣轉換。
        /// </summary>
        public SqlParameterExpression TransformParameter(SqlParameterExpression parameter, Func<object, object> transform)
        {
            var binding = this.parameterBindings[parameter.Name];

            this.parameterBindings[parameter.Name] = values => transform(binding(values));

            return new SqlParameterExpression(parameter.Name, transform(parameter.Value), parameter.Type, parameter.Property);
        }
    }
}
