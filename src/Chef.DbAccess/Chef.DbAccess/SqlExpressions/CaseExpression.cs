using System;
using System.Collections.Generic;
using System.Linq;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     <c>CASE WHEN [m].[Age] &gt; @Age_0 THEN [m].[Name] ELSE @Name_0 END</c>。<see cref="ElseResult" /> 為 <c>null</c> 時不輸出 <c>ELSE</c>。
    /// </summary>
    public sealed class CaseExpression : SqlExpression
    {
        public CaseExpression(IEnumerable<CaseWhenClause> whenClauses, SqlExpression elseResult, Type type)
            : base(type)
        {
            this.WhenClauses = whenClauses?.ToArray() ?? throw new ArgumentNullException(nameof(whenClauses));
            this.ElseResult = elseResult;

            if (this.WhenClauses.Count == 0) throw new ArgumentException("Must be at least one when clause.", nameof(whenClauses));
            if (this.WhenClauses.Any(w => w == null)) throw new ArgumentException("When clauses can not contain null.", nameof(whenClauses));
        }

        public IReadOnlyList<CaseWhenClause> WhenClauses { get; }

        public SqlExpression ElseResult { get; }
    }

    /// <summary>
    ///     <c>WHEN {Test} THEN {Result}</c>
    /// </summary>
    public sealed class CaseWhenClause
    {
        public CaseWhenClause(SqlExpression test, SqlExpression result)
        {
            this.Test = test ?? throw new ArgumentNullException(nameof(test));
            this.Result = result ?? throw new ArgumentNullException(nameof(result));
        }

        public SqlExpression Test { get; }

        public SqlExpression Result { get; }
    }
}
