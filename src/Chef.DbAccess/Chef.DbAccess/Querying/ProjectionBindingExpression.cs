using System;
using System.Linq.Expressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     Shaper 中的佔位符號：查詢結果第 <see cref="Index" /> 個欄位的值（對應 <c>SelectExpression.Projections[Index]</c>）。
    ///     之後編譯 Shaper 時換成從 <c>DbDataReader</c> 讀值。
    /// </summary>
    internal sealed class ProjectionBindingExpression : Expression
    {
        public ProjectionBindingExpression(int index, Type type)
        {
            this.Index = index;
            this.Type = type ?? throw new ArgumentNullException(nameof(type));
        }

        public int Index { get; }

        public override ExpressionType NodeType => ExpressionType.Extension;

        public override Type Type { get; }

        public override string ToString()
        {
            return $"Projection({this.Index})";
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            return this;
        }
    }
}
