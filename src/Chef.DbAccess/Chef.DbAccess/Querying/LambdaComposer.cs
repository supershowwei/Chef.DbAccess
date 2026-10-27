using System.Linq;
using System.Linq.Expressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把 <c>Select</c> 之後的 lambda 改寫成以原本實體為參數的 lambda，例如
    ///     <c>Select(x =&gt; new { N = x.Name })</c> 之後的 <c>a =&gt; a.N == "J"</c> 改寫成 <c>x =&gt; x.Name == "J"</c>，
    ///     翻譯器就可以照常把成員對應到欄位。
    ///     <para>
    ///         Join 的結果選擇器有兩個參數，例如 <c>(m, d) =&gt; new { m, d }</c>，每個參數各自換成一邊的投影，
    ///         組出來的 lambda 以兩邊所有的實體為參數，例如 <c>(x1, x2) =&gt; new { m = x1, d = x2 }</c>。
    ///     </para>
    /// </summary>
    internal static class LambdaComposer
    {
        /// <param name="lambda">接在 <c>Select</c> 之後的 lambda，例如 <c>a =&gt; a.N == "J"</c>。</param>
        /// <param name="selectors">
        ///     <paramref name="lambda" /> 每個參數目前的投影，例如 <c>x =&gt; new { N = x.Name }</c>；組出來的 lambda 依序以這些投影的參數為參數。
        /// </param>
        public static LambdaExpression Compose(LambdaExpression lambda, params LambdaExpression[] selectors)
        {
            var body = lambda.Body;

            for (var i = 0; i < selectors.Length; i++)
            {
                body = new ParameterReplacer(lambda.Parameters[i], selectors[i].Body).Visit(body);
            }

            return Expression.Lambda(new ProjectionMemberSimplifier().Visit(body), selectors.SelectMany(s => s.Parameters));
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
