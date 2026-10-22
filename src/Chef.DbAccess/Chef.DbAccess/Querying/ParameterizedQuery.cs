using System.Collections.Generic;
using System.Linq.Expressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     <see cref="PartialEvaluator" /> 處理過的查詢運算式：可以在用戶端算出的值已經換成具名的
    ///     <see cref="ParameterExpression" />，值另外放在 <see cref="ParameterValues" />。
    /// </summary>
    internal sealed class ParameterizedQuery
    {
        public ParameterizedQuery(Expression expression, IReadOnlyDictionary<string, object> parameterValues)
        {
            this.Expression = expression;
            this.ParameterValues = parameterValues;
        }

        /// <summary>
        ///     參數化後的運算式。只要「形狀」相同，不論閉包變數的值是多少，這棵樹都長得一樣，可以拿來當查詢快取的鍵。
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        ///     參數名稱（<see cref="ParameterExpression.Name" />）對應到這次執行的值。
        /// </summary>
        public IReadOnlyDictionary<string, object> ParameterValues { get; }
    }
}
