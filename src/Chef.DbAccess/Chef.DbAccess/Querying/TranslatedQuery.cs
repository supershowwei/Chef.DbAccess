using System.Linq.Expressions;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     <see cref="QueryableMethodTranslator" /> 的結果：要執行的 SQL AST，以及把每一列組成結果物件的 Shaper。
    /// </summary>
    internal sealed class TranslatedQuery
    {
        public TranslatedQuery(SelectExpression select, Expression shaper)
        {
            this.Select = select;
            this.Shaper = shaper;
        }

        public SelectExpression Select { get; }

        /// <summary>
        ///     組出一筆結果的運算式，例如 <c>new { Id = Projection(0), N = Projection(1) }</c>、
        ///     <c>new Member { Id = Projection(0), Name = Projection(1) }</c>。
        ///     其中 <see cref="ProjectionBindingExpression" /> 是查詢結果的欄位；不依賴欄位的常數與查詢參數（例如 <c>Tag = tag</c>）原樣保留，在用戶端取值。
        /// </summary>
        public Expression Shaper { get; }
    }
}
