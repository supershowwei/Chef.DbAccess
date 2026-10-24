using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     Chef.DbAccess 的 <see cref="IQueryable{T}" />。查詢的根是值為自己的常數，之後的 LINQ 運算子由 <see cref="ChefQueryProvider" /> 組成新的查詢；
    ///     列舉時才翻譯成 SQL 並執行。
    /// </summary>
    internal sealed class ChefQueryable<T> : IOrderedQueryable<T>
    {
        /// <summary>
        ///     查詢的根，例如 <c>members.Query()</c>。
        /// </summary>
        public ChefQueryable(ChefQueryProvider provider)
        {
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Expression = Expression.Constant(this);
        }

        public ChefQueryable(ChefQueryProvider provider, Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));
            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type)) throw new ArgumentException($"Expression must be IQueryable<{typeof(T).Name}>.", nameof(expression));

            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Expression = expression;
        }

        public Type ElementType => typeof(T);

        public Expression Expression { get; }

        public ChefQueryProvider Provider { get; }

        IQueryProvider IQueryable.Provider => this.Provider;

        public IEnumerator<T> GetEnumerator()
        {
            return this.Provider.ExecuteSequence<T>(this.Expression).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        ///     除錯器顯示的文字；不執行查詢，也不翻譯（翻譯可能丟出例外）。
        /// </summary>
        public override string ToString()
        {
            return this.Expression.NodeType == ExpressionType.Constant ? $"ChefQueryable<{typeof(T).Name}>" : this.Expression.ToString();
        }
    }
}
