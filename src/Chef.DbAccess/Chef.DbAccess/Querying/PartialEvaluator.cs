using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     先在用戶端算出查詢運算式中不依賴 lambda 參數的子樹（閉包變數、<c>DateTime.Today.AddDays(-7)</c>、<c>GetId()</c>……）。
    ///     <list type="bullet">
    ///         <item>每次執行都可能不同的值（含成員存取或方法呼叫的子樹）換成具名的 <see cref="ParameterExpression" />，讓查詢的形狀固定，可以快取。</item>
    ///         <item>只由常數組成的子樹（例如 <c>new DateTime(2020, 1, 1)</c>）摺疊成 <see cref="ConstantExpression" />，原本就是常數的維持不變。</item>
    ///         <item>算出來是 <see cref="IQueryable" /> 的子樹（例如 <c>departments.Query()</c>），換成它的運算式，之後可以翻譯成子查詢。</item>
    ///         <item><see cref="Queryable" /> 運算子的常數引數（<c>Skip(20)</c>、<c>Take(10)</c>）一律換成參數。</item>
    ///     </list>
    ///     <para>
    ///         值是 <see cref="IQueryable" /> 的常數（查詢的根）與 <see cref="Queryable" /> 的運算子要留給翻譯器，包含它們的子樹不會在用戶端求值，
    ///         所以 <c>departments.Query().Any(...)</c> 會變成子查詢，而不是先在用戶端執行。
    ///     </para>
    /// </summary>
    internal sealed class PartialEvaluator : ExpressionVisitor
    {
        private readonly HashSet<Expression> candidates;
        private readonly Dictionary<string, object> parameterValues;

        private PartialEvaluator(HashSet<Expression> candidates, Dictionary<string, object> parameterValues)
        {
            this.candidates = candidates;
            this.parameterValues = parameterValues;
        }

        public static ParameterizedQuery Evaluate(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            var parameterValues = new Dictionary<string, object>();

            return new ParameterizedQuery(Evaluate(expression, parameterValues), parameterValues);
        }

        public override Expression Visit(Expression node)
        {
            if (node == null || node.NodeType == ExpressionType.Extension) return node;

            if (!this.candidates.Contains(node)) return base.Visit(node);

            if (node is ConstantExpression) return node;

            if (!VariableFinder.Contains(node)) return Expression.Constant(EvaluateValue(node), node.Type);

            var value = EvaluateValue(node);

            if (value is IQueryable queryable) return Evaluate(queryable.Expression, this.parameterValues);

            return this.CreateParameter(node.Type, value, GetNameHint(node));
        }

        /// <summary>
        ///     <c>Skip(skip)</c>、<c>Take(10)</c> 這類查詢運算子的引數（lambda 以外）在建立查詢時就已經算成常數，
        ///     看不出原本是變數還是寫死的數字，所以一律換成以方法命名的參數（例如 <c>__Skip_0</c>），不同頁數才能共用同一個快取。
        /// </summary>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(Queryable)) return base.VisitMethodCall(node);

            var arguments = node.Arguments.Select(
                    a => a is ConstantExpression constant && !(constant.Value is IQueryable)
                             ? this.CreateParameter(constant.Type, constant.Value, node.Method.Name)
                             : this.Visit(a))
                .ToArray();

            return node.Update(this.Visit(node.Object), arguments);
        }

        /// <summary>
        ///     <c>new MemberDto { Key = x.Id }</c> 裡的 <c>new MemberDto()</c> 不依賴 lambda 參數，但必須維持是建構式呼叫，不能換成常數或參數，只走訪它的引數。
        /// </summary>
        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            return node.Update(this.VisitNewArguments(node.NewExpression), node.Bindings.Select(this.VisitMemberBinding));
        }

        protected override Expression VisitListInit(ListInitExpression node)
        {
            return node.Update(this.VisitNewArguments(node.NewExpression), node.Initializers.Select(this.VisitElementInit));
        }

        private NewExpression VisitNewArguments(NewExpression node)
        {
            return node.Update(this.Visit(node.Arguments));
        }

        private ParameterExpression CreateParameter(Type type, object value, string nameHint)
        {
            var name = string.Concat("__", nameHint, "_", this.parameterValues.Count.ToString());

            this.parameterValues.Add(name, value);

            return Expression.Parameter(type, name);
        }

        private static Expression Evaluate(Expression expression, Dictionary<string, object> parameterValues)
        {
            return new PartialEvaluator(Nominator.Nominate(expression), parameterValues).Visit(expression);
        }

        private static object EvaluateValue(Expression expression)
        {
            switch (expression)
            {
                case ConstantExpression constant: return constant.Value;

                // 閉包變數是最常見的情況，用反射取值，避免每次執行都編譯 delegate。
                case MemberExpression member when member.Member is FieldInfo || member.Member is PropertyInfo:
                    var instance = member.Expression == null ? null : EvaluateValue(member.Expression);

                    // 執行個體是 null 時交給編譯後的 delegate 丟出和 C# 一樣的 NullReferenceException。
                    if (member.Expression != null && instance == null) break;

                    if (member.Member is FieldInfo field) return field.GetValue(instance);

                    try
                    {
                        return ((PropertyInfo)member.Member).GetValue(instance);
                    }
                    catch (TargetInvocationException ex) when (ex.InnerException != null)
                    {
                        ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                        throw;
                    }
            }

            return Expression.Lambda<Func<object>>(Expression.Convert(expression, typeof(object))).Compile()();
        }

        /// <summary>
        ///     參數名稱取自成員或方法名稱，例如閉包變數 <c>id</c> 會變成 <c>__id_0</c>；編譯器產生的名稱只保留英數字與底線。
        /// </summary>
        private static string GetNameHint(Expression expression)
        {
            string name;

            switch (expression)
            {
                case MemberExpression member:
                    name = member.Member.Name;
                    break;

                case MethodCallExpression methodCall:
                    name = methodCall.Method.Name;
                    break;

                default:
                    name = null;
                    break;
            }

            name = new string((name ?? string.Empty).Where(c => char.IsLetterOrDigit(c) || c == '_').ToArray());

            return name.Length == 0 ? "p" : name;
        }

        /// <summary>
        ///     由下往上找出可以在用戶端求值的節點：子樹裡參考到的 lambda 參數，都是在子樹內部宣告的。
        ///     所以 <c>ids.Where(i =&gt; i &gt; 0).Count()</c> 可以求值，<c>ids.Any(i =&gt; i == x.Id)</c> 不行。
        /// </summary>
        private sealed class Nominator : ExpressionVisitor
        {
            private static readonly ConcurrentDictionary<Type, bool> ByRefLikeTypes = new ConcurrentDictionary<Type, bool>();

            private readonly HashSet<Expression> candidates = new HashSet<Expression>();
            private readonly List<IReadOnlyCollection<ParameterExpression>> scopes = new List<IReadOnlyCollection<ParameterExpression>>();

            // 目前子樹參考到的參數中，宣告它的 lambda 最外層是第幾層（1 起算；0 表示不是任何 lambda 宣告的參數）。
            private int outermostLevel;

            private bool containsQueryNode;

            public static HashSet<Expression> Nominate(Expression expression)
            {
                var nominator = new Nominator();

                nominator.Visit(expression);

                return nominator.candidates;
            }

            public override Expression Visit(Expression node)
            {
                if (node == null) return null;

                var parentOutermostLevel = this.outermostLevel;
                var parentContainsQueryNode = this.containsQueryNode;

                this.outermostLevel = int.MaxValue;
                this.containsQueryNode = IsQueryNode(node);

                // 自訂節點不一定能走訪子節點，整個留給翻譯器。
                if (node.NodeType != ExpressionType.Extension) base.Visit(node);

                // 節點外面包著 scopes.Count 層 lambda，參考到的參數都宣告在更內層時，才和外面的 lambda 參數無關。
                if (!this.containsQueryNode && this.outermostLevel > this.scopes.Count && CanBeEvaluated(node)) this.candidates.Add(node);

                this.outermostLevel = Math.Min(this.outermostLevel, parentOutermostLevel);
                this.containsQueryNode |= parentContainsQueryNode;

                return node;
            }

            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                this.scopes.Add(node.Parameters);

                try
                {
                    return base.VisitLambda(node);
                }
                finally
                {
                    this.scopes.RemoveAt(this.scopes.Count - 1);
                }
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                var level = this.scopes.FindLastIndex(s => s.Contains(node)) + 1;

                this.outermostLevel = Math.Min(this.outermostLevel, level);

                return node;
            }

            /// <summary>
            ///     查詢的根（值是 <see cref="IQueryable" /> 的常數、自訂節點）或 <see cref="Queryable" /> 的運算子。
            /// </summary>
            private static bool IsQueryNode(Expression node)
            {
                switch (node)
                {
                    case ConstantExpression constant: return constant.Value is IQueryable;
                    case MethodCallExpression methodCall: return methodCall.Method.DeclaringType == typeof(Queryable);
                    default: return node.NodeType == ExpressionType.Extension;
                }
            }

            /// <summary>
            ///     lambda 本身不求值（但包含它的呼叫可以），參數則一定要由外面提供值。
            ///     <para>
            ///         <c>ReadOnlySpan&lt;T&gt;</c> 這類 ref struct 不能裝箱成 <see cref="object" />，所以不求值，改為求值裡面的陣列。
            ///         C# 14 會把 <c>ids.Contains(x.Id)</c> 綁定成 <c>MemoryExtensions.Contains(ReadOnlySpan&lt;int&gt;.op_Implicit(ids), x.Id)</c>。
            ///     </para>
            /// </summary>
            private static bool CanBeEvaluated(Expression node)
            {
                switch (node.NodeType)
                {
                    case ExpressionType.Parameter:
                    case ExpressionType.Lambda:
                    case ExpressionType.Quote:
                        return false;

                    default: return !IsByRefLike(node.Type);
                }
            }

            private static bool IsByRefLike(Type type)
            {
                // netstandard2.0 沒有 Type.IsByRefLike，改看編譯器加上的 IsByRefLikeAttribute。
                return type.IsValueType
                       && ByRefLikeTypes.GetOrAdd(
                           type,
                           t => t.GetCustomAttributes(false).Any(a => a.GetType().FullName == "System.Runtime.CompilerServices.IsByRefLikeAttribute"));
            }
        }

        /// <summary>
        ///     找出子樹裡是否有成員存取或方法呼叫；有的話每次執行的值可能不同，要換成參數而不是摺疊成常數。
        /// </summary>
        private sealed class VariableFinder : ExpressionVisitor
        {
            private bool found;

            public static bool Contains(Expression expression)
            {
                var finder = new VariableFinder();

                finder.Visit(expression);

                return finder.found;
            }

            public override Expression Visit(Expression node)
            {
                if (this.found || node == null) return node;

                switch (node.NodeType)
                {
                    case ExpressionType.MemberAccess:
                    case ExpressionType.Call:
                    case ExpressionType.Invoke:
                        this.found = true;
                        return node;

                    default: return base.Visit(node);
                }
            }
        }
    }
}
