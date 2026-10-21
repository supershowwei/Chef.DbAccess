using System;
using System.Collections.Generic;
using System.Linq;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     <c>[m].[No] IN (@Id_0, @Id_1)</c>，<see cref="IsNegated" /> 為 <c>true</c> 時是 <c>NOT IN</c>。
    ///     <para>值清單是空的時候，由方言輸出成恆假（或恆真）的條件。</para>
    /// </summary>
    public sealed class InExpression : SqlExpression
    {
        public InExpression(SqlExpression item, IEnumerable<SqlExpression> values, bool isNegated = false)
            : base(typeof(bool))
        {
            this.Item = item ?? throw new ArgumentNullException(nameof(item));
            this.Values = values?.ToArray() ?? throw new ArgumentNullException(nameof(values));
            this.IsNegated = isNegated;

            if (this.Values.Any(v => v == null)) throw new ArgumentException("Values can not contain null.", nameof(values));
        }

        public SqlExpression Item { get; }

        public IReadOnlyList<SqlExpression> Values { get; }

        public bool IsNegated { get; }
    }
}
