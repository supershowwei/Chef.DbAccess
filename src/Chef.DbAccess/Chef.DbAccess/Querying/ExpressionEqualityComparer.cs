using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     依「形狀」比較兩棵運算式樹，用在查詢快取。
    ///     <list type="bullet">
    ///         <item>lambda 參數依位置比較，不看名稱，所以 <c>x =&gt; x.Age</c> 與 <c>y =&gt; y.Age</c> 相等。</item>
    ///         <item>不屬於任何 lambda 的參數（<see cref="PartialEvaluator" /> 產生的 <c>__age_0</c>）依名稱比較。</item>
    ///         <item>常數依值比較；集合依內容比較，因為同一段程式每次執行都會建立新的陣列。</item>
    ///         <item>
    ///             值是 <see cref="IQueryable" /> 的常數（查詢的根）只比較型別與元素型別，因為每次 <c>Query()</c> 都是新的執行個體；
    ///             所以根上的其他狀態不能影響產生的 SQL。
    ///         </item>
    ///     </list>
    ///     <para>不認得的節點只有同一個執行個體才相等，頂多快取不到，不會誤用別的查詢的 SQL。</para>
    /// </summary>
    internal sealed class ExpressionEqualityComparer : IEqualityComparer<Expression>
    {
        public static readonly ExpressionEqualityComparer Instance = new ExpressionEqualityComparer();

        private ExpressionEqualityComparer()
        {
        }

        public bool Equals(Expression x, Expression y)
        {
            return new Comparison().Compare(x, y);
        }

        public int GetHashCode(Expression obj)
        {
            if (obj == null) return 0;

            var hasher = new Hasher();

            hasher.Visit(obj);

            return hasher.HashCode;
        }

        private static bool ConstantEquals(object x, object y)
        {
            if (x is IQueryable queryableX && y is IQueryable queryableY)
            {
                return queryableX.GetType() == queryableY.GetType() && queryableX.ElementType == queryableY.ElementType;
            }

            if (x is IEnumerable enumerableX && y is IEnumerable enumerableY && !(x is string))
            {
                return enumerableX.Cast<object>().SequenceEqual(enumerableY.Cast<object>());
            }

            return Equals(x, y);
        }

        private static int GetConstantHashCode(object value)
        {
            switch (value)
            {
                case null: return 0;
                case IQueryable queryable: return Combine(queryable.GetType().GetHashCode(), queryable.ElementType.GetHashCode());
                case string str: return str.GetHashCode();
                case IEnumerable enumerable: return enumerable.Cast<object>().Aggregate(17, (hash, item) => Combine(hash, item?.GetHashCode() ?? 0));
                default: return value.GetHashCode();
            }
        }

        private static int Combine(int hash, int value)
        {
            unchecked
            {
                return (hash * 31) + value;
            }
        }

        private sealed class Comparison
        {
            // 目前所在的 lambda 宣告的參數，x 與 y 兩邊依位置對應。
            private readonly List<ParameterExpression> parametersX = new List<ParameterExpression>();
            private readonly List<ParameterExpression> parametersY = new List<ParameterExpression>();

            public bool Compare(Expression x, Expression y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x == null || y == null) return false;
                if (x.NodeType != y.NodeType || x.Type != y.Type) return false;

                switch (x)
                {
                    case BinaryExpression binary:
                        return this.CompareBinary(binary, (BinaryExpression)y);

                    case UnaryExpression unary:
                        var unaryY = (UnaryExpression)y;
                        return Equals(unary.Method, unaryY.Method) && this.Compare(unary.Operand, unaryY.Operand);

                    case ConstantExpression constant:
                        return ConstantEquals(constant.Value, ((ConstantExpression)y).Value);

                    case ParameterExpression parameter:
                        return this.CompareParameter(parameter, (ParameterExpression)y);

                    case MemberExpression member:
                        var memberY = (MemberExpression)y;
                        return Equals(member.Member, memberY.Member) && this.Compare(member.Expression, memberY.Expression);

                    case MethodCallExpression methodCall:
                        var methodCallY = (MethodCallExpression)y;
                        return Equals(methodCall.Method, methodCallY.Method)
                               && this.Compare(methodCall.Object, methodCallY.Object)
                               && this.CompareList(methodCall.Arguments, methodCallY.Arguments);

                    case LambdaExpression lambda:
                        return this.CompareLambda(lambda, (LambdaExpression)y);

                    case NewExpression @new:
                        return this.CompareNew(@new, (NewExpression)y);

                    case NewArrayExpression newArray:
                        return this.CompareList(newArray.Expressions, ((NewArrayExpression)y).Expressions);

                    case MemberInitExpression memberInit:
                        var memberInitY = (MemberInitExpression)y;
                        return this.CompareNew(memberInit.NewExpression, memberInitY.NewExpression)
                               && CompareList(memberInit.Bindings, memberInitY.Bindings, this.CompareBinding);

                    case ListInitExpression listInit:
                        var listInitY = (ListInitExpression)y;
                        return this.CompareNew(listInit.NewExpression, listInitY.NewExpression)
                               && CompareList(listInit.Initializers, listInitY.Initializers, this.CompareElementInit);

                    case ConditionalExpression conditional:
                        var conditionalY = (ConditionalExpression)y;
                        return this.Compare(conditional.Test, conditionalY.Test)
                               && this.Compare(conditional.IfTrue, conditionalY.IfTrue)
                               && this.Compare(conditional.IfFalse, conditionalY.IfFalse);

                    case TypeBinaryExpression typeBinary:
                        var typeBinaryY = (TypeBinaryExpression)y;
                        return typeBinary.TypeOperand == typeBinaryY.TypeOperand && this.Compare(typeBinary.Expression, typeBinaryY.Expression);

                    case InvocationExpression invocation:
                        var invocationY = (InvocationExpression)y;
                        return this.Compare(invocation.Expression, invocationY.Expression) && this.CompareList(invocation.Arguments, invocationY.Arguments);

                    case IndexExpression index:
                        var indexY = (IndexExpression)y;
                        return Equals(index.Indexer, indexY.Indexer) && this.Compare(index.Object, indexY.Object) && this.CompareList(index.Arguments, indexY.Arguments);

                    case DefaultExpression _:
                        return true;

                    default: return false;
                }
            }

            private static bool CompareList<T>(IReadOnlyList<T> x, IReadOnlyList<T> y, Func<T, T, bool> compare)
            {
                if (x.Count != y.Count) return false;

                for (var i = 0; i < x.Count; i++)
                {
                    if (!compare(x[i], y[i])) return false;
                }

                return true;
            }

            private bool CompareList(ReadOnlyCollection<Expression> x, ReadOnlyCollection<Expression> y)
            {
                return CompareList(x, y, this.Compare);
            }

            private bool CompareBinary(BinaryExpression x, BinaryExpression y)
            {
                return Equals(x.Method, y.Method)
                       && x.IsLiftedToNull == y.IsLiftedToNull
                       && this.Compare(x.Left, y.Left)
                       && this.Compare(x.Right, y.Right)
                       && this.Compare(x.Conversion, y.Conversion);
            }

            /// <summary>
            ///     兩邊都是 lambda 宣告的參數時要在同一個位置；都不是時比較名稱；只有一邊是則不相等。
            /// </summary>
            private bool CompareParameter(ParameterExpression x, ParameterExpression y)
            {
                var indexX = this.parametersX.LastIndexOf(x);
                var indexY = this.parametersY.LastIndexOf(y);

                if (indexX >= 0 || indexY >= 0) return indexX == indexY;

                return x.Name == y.Name;
            }

            private bool CompareLambda(LambdaExpression x, LambdaExpression y)
            {
                // 參數型別已經包含在 lambda 的委派型別中，這裡只需要建立對應。
                var count = this.parametersX.Count;

                this.parametersX.AddRange(x.Parameters);
                this.parametersY.AddRange(y.Parameters);

                try
                {
                    return this.Compare(x.Body, y.Body);
                }
                finally
                {
                    this.parametersX.RemoveRange(count, this.parametersX.Count - count);
                    this.parametersY.RemoveRange(count, this.parametersY.Count - count);
                }
            }

            private bool CompareNew(NewExpression x, NewExpression y)
            {
                if (!Equals(x.Constructor, y.Constructor)) return false;
                if (x.Members == null || y.Members == null) return x.Members == y.Members && this.CompareList(x.Arguments, y.Arguments);

                return x.Members.SequenceEqual(y.Members) && this.CompareList(x.Arguments, y.Arguments);
            }

            private bool CompareBinding(MemberBinding x, MemberBinding y)
            {
                if (x.BindingType != y.BindingType || !Equals(x.Member, y.Member)) return false;

                switch (x)
                {
                    case MemberAssignment assignment:
                        return this.Compare(assignment.Expression, ((MemberAssignment)y).Expression);

                    case MemberMemberBinding memberBinding:
                        return CompareList(memberBinding.Bindings, ((MemberMemberBinding)y).Bindings, this.CompareBinding);

                    case MemberListBinding listBinding:
                        return CompareList(listBinding.Initializers, ((MemberListBinding)y).Initializers, this.CompareElementInit);

                    default: return false;
                }
            }

            private bool CompareElementInit(ElementInit x, ElementInit y)
            {
                return Equals(x.AddMethod, y.AddMethod) && this.CompareList(x.Arguments, y.Arguments);
            }
        }

        /// <summary>
        ///     走訪整棵樹，把節點類型、型別與成員、方法、常數等資訊組合成雜湊值。
        ///     參數只看型別，因為 lambda 參數不比較名稱；相等的運算式一定會得到相同的雜湊值。
        /// </summary>
        private sealed class Hasher : ExpressionVisitor
        {
            public int HashCode { get; private set; } = 17;

            public override Expression Visit(Expression node)
            {
                if (node == null) return null;

                this.Add(((int)node.NodeType).GetHashCode());
                this.Add(node.Type.GetHashCode());

                // 自訂節點不一定能走訪子節點，只有同一個執行個體才相等。
                if (node.NodeType == ExpressionType.Extension)
                {
                    this.Add(RuntimeHelpers.GetHashCode(node));

                    return node;
                }

                return base.Visit(node);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                this.Add(GetConstantHashCode(node.Value));

                return node;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                this.Add(node.Member);

                return base.VisitMember(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                this.Add(node.Method);

                return base.VisitMethodCall(node);
            }

            protected override Expression VisitBinary(BinaryExpression node)
            {
                this.Add(node.Method);

                return base.VisitBinary(node);
            }

            protected override Expression VisitUnary(UnaryExpression node)
            {
                this.Add(node.Method);

                return base.VisitUnary(node);
            }

            protected override Expression VisitNew(NewExpression node)
            {
                this.Add(node.Constructor);

                return base.VisitNew(node);
            }

            protected override MemberBinding VisitMemberBinding(MemberBinding node)
            {
                this.Add(((int)node.BindingType).GetHashCode());
                this.Add(node.Member);

                return base.VisitMemberBinding(node);
            }

            protected override ElementInit VisitElementInit(ElementInit node)
            {
                this.Add(node.AddMethod);

                return base.VisitElementInit(node);
            }

            protected override Expression VisitTypeBinary(TypeBinaryExpression node)
            {
                this.Add(node.TypeOperand.GetHashCode());

                return base.VisitTypeBinary(node);
            }

            private void Add(MemberInfo member)
            {
                this.Add(member?.GetHashCode() ?? 0);
            }

            private void Add(int value)
            {
                this.HashCode = Combine(this.HashCode, value);
            }
        }
    }
}
