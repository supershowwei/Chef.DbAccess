using System.Linq;
using System.Linq.Expressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把 <c>Select</c> 之後的 lambda 改寫成以原本實體為參數的 lambda，例如
    ///     <c>Select(x =&gt; new { N = x.Name })</c> 之後的 <c>a =&gt; a.N == "J"</c> 改寫成 <c>x =&gt; x.Name == "J"</c>，
    ///     翻譯器就可以照常把成員對應到欄位。
    /// </summary>
    internal static class LambdaComposer
    {
        /// <param name="lambda">接在 <c>Select</c> 之後的 lambda，例如 <c>a =&gt; a.N == "J"</c>。</param>
        /// <param name="selector">目前的投影，例如 <c>x =&gt; new { N = x.Name }</c>。</param>
        public static LambdaExpression Compose(LambdaExpression lambda, LambdaExpression selector)
        {
            var body = new ParameterReplacer(lambda.Parameters[0], selector.Body).Visit(lambda.Body);

            return Expression.Lambda(new ProjectionMemberSimplifier().Visit(body), selector.Parameters);
        }

        private sealed class ParameterReplacer : ExpressionVisitor
        {
            private readonly ParameterExpression parameter;
            private readonly Expression replacement;

            public ParameterReplacer(ParameterExpression parameter, Expression replacement)
            {
                this.parameter = parameter;
                this.replacement = replacement;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                return node == this.parameter ? this.replacement : node;
            }
        }

        /// <summary>
        ///     <c>new { N = x.Name }.N</c> → <c>x.Name</c>、<c>new Dto { Title = x.Name }.Title</c> → <c>x.Name</c>。
        ///     投影中沒有指定的成員（例如只用建構函式的 DTO）維持原樣，之後由翻譯器丟出無法翻譯的例外。
        /// </summary>
        private sealed class ProjectionMemberSimplifier : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                var instance = this.Visit(node.Expression);
                var name = node.Member.Name;

                switch (instance)
                {
                    case NewExpression @new when @new.Members != null:
                        // 匿名型別的成員可能是屬性或 getter 方法。
                        var index = @new.Members.ToList().FindIndex(m => m.Name == name || m.Name == "get_" + name);

                        if (index >= 0) return @new.Arguments[index];

                        break;

                    case MemberInitExpression memberInit:
                        var assignment = memberInit.Bindings.OfType<MemberAssignment>().FirstOrDefault(b => b.Member.Name == name);

                        if (assignment != null) return assignment.Expression;

                        break;
                }

                return node.Update(instance);
            }
        }
    }
}
