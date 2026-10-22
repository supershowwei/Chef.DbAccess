using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     查詢結果要怎麼交給呼叫端。
    /// </summary>
    internal enum QueryResultKind
    {
        /// <summary>
        ///     所有資料列（<c>ToList</c>、<c>foreach</c>）。
        /// </summary>
        Sequence,

        /// <summary>
        ///     第一筆，沒有資料時丟出例外。SQL 只取一筆（<c>TOP(1)</c>）。
        /// </summary>
        First,

        FirstOrDefault,

        /// <summary>
        ///     唯一的一筆，沒有或超過一筆時丟出例外。SQL 取兩筆（<c>TOP(2)</c>）才能判斷是否超過一筆。
        /// </summary>
        Single,

        SingleOrDefault,

        /// <summary>
        ///     單一值，例如 <c>Count</c> 的 <c>COUNT(*)</c>。
        /// </summary>
        Scalar,

        /// <summary>
        ///     有任何資料列時為 <c>true</c>。SQL 只取一筆（<c>TOP(1)</c>）。
        /// </summary>
        Any,

        /// <summary>
        ///     沒有任何資料列時為 <c>true</c>：SQL 找出一筆「不符合條件」的資料（<c>WHERE NOT (...)</c>）。
        /// </summary>
        All
    }

    /// <summary>
    ///     <see cref="QueryableMethodTranslator" /> 的結果：要執行的 SQL AST、把每一列組成結果物件的 Shaper，以及結果的種類。
    /// </summary>
    internal sealed class TranslatedQuery
    {
        public TranslatedQuery(
            SelectExpression select,
            Expression shaper,
            QueryResultKind resultKind,
            IReadOnlyDictionary<string, Func<IReadOnlyDictionary<string, object>, object>> parameterBindings)
        {
            this.Select = select;
            this.Shaper = shaper;
            this.ResultKind = resultKind;
            this.ParameterBindings = parameterBindings;
        }

        public SelectExpression Select { get; }

        /// <summary>
        ///     組出一筆結果的運算式，例如 <c>new { Id = Projection(0), N = Projection(1) }</c>、
        ///     <c>new Member { Id = Projection(0), Name = Projection(1) }</c>。
        ///     其中 <see cref="ProjectionBindingExpression" /> 是查詢結果的欄位；不依賴欄位的常數與查詢參數（例如 <c>Tag = tag</c>）原樣保留，在用戶端取值。
        ///     <see cref="QueryResultKind.Any" /> 與 <see cref="QueryResultKind.All" /> 只看有沒有資料列，為 <c>null</c>。
        /// </summary>
        public Expression Shaper { get; }

        public QueryResultKind ResultKind { get; }

        /// <summary>
        ///     SQL 參數名稱對應到「依查詢參數值算出 SQL 參數值」的方式（見 <see cref="SqlTranslationContext.ParameterBindings" />），
        ///     快取命中時用新的查詢參數值重新算出每個 SQL 參數的值。
        /// </summary>
        public IReadOnlyDictionary<string, Func<IReadOnlyDictionary<string, object>, object>> ParameterBindings { get; }
    }
}
