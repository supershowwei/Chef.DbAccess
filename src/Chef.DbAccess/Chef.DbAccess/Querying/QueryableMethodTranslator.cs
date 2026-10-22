using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把 <see cref="Queryable" /> 運算子組成的查詢（已經過 <see cref="PartialEvaluator" />）翻譯成 <see cref="SelectExpression" /> 與 Shaper。
    ///     <list type="bullet">
    ///         <item>查詢的根是值為 <see cref="IQueryable" /> 的常數，依 <see cref="IQueryable.ElementType" /> 對應資料表；沒有 <c>Select</c> 時選出所有對應的欄位。</item>
    ///         <item>多次 <c>Where</c> 以 <c>AND</c> 結合（3.x 的 <c>Where</c> 會覆蓋前一個條件）。</item>
    ///         <item><c>OrderBy</c> 會取代先前的排序，<c>ThenBy</c> 接在後面，與 LINQ 的語意相同。</item>
    ///         <item>
    ///             <c>Skip</c> / <c>Take</c> 的數字由 <see cref="PartialEvaluator" /> 換成參數，SQL Server 輸出 <c>OFFSET {=Skip_0} ROWS</c>，
    ///             執行時由 Dapper 寫入數字，送到資料庫的 SQL 與 3.x 相同。
    ///         </item>
    ///         <item>
    ///             <c>Select</c> 可以投影成實體、匿名型別、DTO 或單一值；之後的 <c>Where</c>、<c>OrderBy</c> 等透過 <see cref="LambdaComposer" />
    ///             對應回欄位，例如 <c>Select(x =&gt; new { N = x.Name }).Where(a =&gt; a.N == "J")</c>。
    ///         </item>
    ///         <item><c>First</c>、<c>Single</c>（含 <c>OrDefault</c>）、<c>Count</c>、<c>LongCount</c>、<c>Any</c>、<c>All</c> 見 <see cref="TranslateResultOperator" />。</item>
    ///     </list>
    ///     <para>
    ///         <c>Skip</c> / <c>Take</c> 之後再接 <c>Where</c>、<c>OrderBy</c>、<c>Distinct</c> 等需要子查詢，目前不支援，丟出 <see cref="NotSupportedException" />。
    ///     </para>
    /// </summary>
    internal sealed class QueryableMethodTranslator
    {
        private readonly SqlTranslationContext context;
        private readonly SqlExpressionTranslator translator;
        private int tableCount;

        public QueryableMethodTranslator(
            SqlTranslationContext context,
            IEnumerable<IMemberTranslator> memberTranslators = null,
            IEnumerable<IMethodCallTranslator> methodCallTranslators = null)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.translator = new SqlExpressionTranslator(context, memberTranslators, methodCallTranslators);
        }

        public TranslatedQuery Translate(Expression query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            if (query is MethodCallExpression methodCall && methodCall.Method.DeclaringType == typeof(Queryable) && TryGetResultKind(methodCall, out var resultKind))
            {
                return this.TranslateResultOperator(methodCall, resultKind);
            }

            return this.Build(this.Visit(query), QueryResultKind.Sequence);
        }

        private static bool TryGetResultKind(MethodCallExpression methodCall, out QueryResultKind resultKind)
        {
            switch (methodCall.Method.Name)
            {
                case nameof(Queryable.First): resultKind = QueryResultKind.First; return true;
                case nameof(Queryable.FirstOrDefault): resultKind = QueryResultKind.FirstOrDefault; return true;
                case nameof(Queryable.Single): resultKind = QueryResultKind.Single; return true;
                case nameof(Queryable.SingleOrDefault): resultKind = QueryResultKind.SingleOrDefault; return true;
                case nameof(Queryable.Count):
                case nameof(Queryable.LongCount):
                    resultKind = QueryResultKind.Scalar; return true;
                case nameof(Queryable.Any): resultKind = QueryResultKind.Any; return true;
                case nameof(Queryable.All): resultKind = QueryResultKind.All; return true;
                default: resultKind = default; return false;
            }
        }

        private TranslatedQuery Build(QueryBuilder source, IEnumerable<ProjectionExpression> projections, Expression shaper, QueryResultKind resultKind)
        {
            return new TranslatedQuery(
                new SelectExpression(projections, new[] { source.Table }, source.Predicate, source.Orderings, source.Offset, source.Limit, source.IsDistinct),
                shaper,
                resultKind,
                this.context.ParameterBindings.ToDictionary(b => b.Key, b => b.Value));
        }

        /// <summary>
        ///     依目前的投影（沒有 <c>Select</c> 時是實體）產生 <c>SELECT</c> 清單與 Shaper。
        /// </summary>
        private TranslatedQuery Build(QueryBuilder source, QueryResultKind resultKind)
        {
            var projection = new ProjectionBuilder(this.translator, source.Table, source.Selector);

            return this.Build(source, projection.Projections, projection.Shaper, resultKind);
        }

        /// <summary>
        ///     結束查詢的運算子：
        ///     <list type="bullet">
        ///         <item><c>First</c> / <c>FirstOrDefault</c> 取一筆（<c>TOP(1)</c>），<c>Single</c> / <c>SingleOrDefault</c> 取兩筆（<c>TOP(2)</c>）才能判斷是否超過一筆。3.x 的 <c>QueryOne</c> 不限制筆數。</item>
        ///         <item><c>Count</c> 輸出 <c>COUNT(*)</c>、<c>LongCount</c> 輸出 <c>COUNT</c> 並回傳 <see cref="long" />（SQL Server 輸出 <c>COUNT_BIG(*)</c>），排序不影響筆數，直接拿掉。</item>
        ///         <item><c>Any</c> 只取一筆看有沒有資料（<c>SELECT TOP(1) 1</c>）；<c>All(predicate)</c> 找一筆不符合的資料（<c>WHERE NOT (predicate)</c>），找不到才是 <c>true</c>。</item>
        ///     </list>
        ///     <para>
        ///         已經 <c>Take</c> 過再取筆數、在 <c>Skip</c> / <c>Take</c> / <c>Distinct</c> 之後 <c>Count</c> 都需要子查詢，目前丟出 <see cref="NotSupportedException" />。
        ///         注意 <c>All</c> 的條件結果是 <c>NULL</c>（例如和 <c>NULL</c> 欄位比較）時，該筆不算不符合，與 C# 不同。
        ///     </para>
        /// </summary>
        private TranslatedQuery TranslateResultOperator(MethodCallExpression methodCall, QueryResultKind resultKind)
        {
            var source = this.Visit(methodCall.Arguments[0]);

            if (methodCall.Arguments.Count == 2)
            {
                source.EnsureNotPaged(methodCall);

                var predicate = this.translator.TranslatePredicate(source.Compose(GetLambda(methodCall.Arguments[1])), source.Table);

                source.AddPredicate(resultKind == QueryResultKind.All ? new SqlUnaryExpression(SqlUnaryOperator.Not, predicate, typeof(bool)) : predicate);
            }
            else if (methodCall.Arguments.Count > 2)
            {
                throw NotTranslatable(methodCall);
            }

            switch (resultKind)
            {
                case QueryResultKind.First:
                case QueryResultKind.FirstOrDefault:
                case QueryResultKind.Single:
                case QueryResultKind.SingleOrDefault:
                    if (source.Limit != null) throw NotTranslatable(methodCall);

                    var isFirst = resultKind == QueryResultKind.First || resultKind == QueryResultKind.FirstOrDefault;

                    source.Limit = new SqlConstantExpression(isFirst ? 1 : 2, typeof(int));

                    return this.Build(source, resultKind);

                case QueryResultKind.Scalar:
                    if (source.Offset != null || source.Limit != null || source.IsDistinct) throw NotTranslatable(methodCall);

                    var type = methodCall.Method.ReturnType;
                    var count = new SqlFunctionExpression("COUNT", new[] { new SqlFragmentExpression("*") }, type);

                    source.Orderings.Clear();

                    return this.Build(source, new[] { new ProjectionExpression(count, "Value") }, new ProjectionBindingExpression(0, type), resultKind);

                default:
                    if (source.Limit != null) throw NotTranslatable(methodCall);

                    source.Limit = new SqlConstantExpression(1, typeof(int));
                    source.Orderings.Clear();

                    return this.Build(source, new[] { new ProjectionExpression(new SqlConstantExpression(1, typeof(int)), "Value") }, null, resultKind);
            }
        }

        /// <summary>
        ///     沿用 3.x 的別名：類別名稱中的大寫字母轉小寫，少於兩個字母時取前三個字母，再加上流水號，例如 <c>Member</c> → <c>mem_1</c>、<c>MemberProfile</c> → <c>mp_1</c>。
        /// </summary>
        private static string CreateAlias(Type type, int number)
        {
            var alias = new string(type.Name.Where(c => c >= 'A' && c <= 'Z').ToArray()).ToLowerInvariant();

            if (alias.Length < 2) alias = (type.Name.Length > 3 ? type.Name.Substring(0, 3) : type.Name).ToLowerInvariant();

            return string.Concat(alias, "_", number.ToString());
        }

        private static LambdaExpression GetLambda(Expression argument)
        {
            return (LambdaExpression)(argument is UnaryExpression quote && quote.NodeType == ExpressionType.Quote ? quote.Operand : argument);
        }

        private static NotSupportedException NotTranslatable(Expression expression)
        {
            return new NotSupportedException($"Can not translate '{expression}' to SQL.");
        }

        private QueryBuilder Visit(Expression expression)
        {
            switch (expression)
            {
                case ConstantExpression constant when constant.Value is IQueryable queryable:
                    return this.CreateRoot(queryable.ElementType);

                case MethodCallExpression methodCall when methodCall.Method.DeclaringType == typeof(Queryable):
                    return this.VisitQueryableMethod(methodCall);

                default: throw NotTranslatable(expression);
            }
        }

        private QueryBuilder CreateRoot(Type elementType)
        {
            var entity = EntityMetadata.For(elementType);

            return new QueryBuilder(TableExpression.For(entity, CreateAlias(elementType, ++this.tableCount)));
        }

        private QueryBuilder VisitQueryableMethod(MethodCallExpression methodCall)
        {
            var source = this.Visit(methodCall.Arguments[0]);
            var arguments = methodCall.Arguments;

            switch (methodCall.Method.Name)
            {
                case nameof(Queryable.Where) when arguments.Count == 2 && GetLambda(arguments[1]).Parameters.Count == 1:
                    source.EnsureNotPaged(methodCall);
                    source.AddPredicate(this.translator.TranslatePredicate(source.Compose(GetLambda(arguments[1])), source.Table));
                    return source;

                case nameof(Queryable.OrderBy) when arguments.Count == 2:
                case nameof(Queryable.OrderByDescending) when arguments.Count == 2:
                    source.EnsureNotPaged(methodCall);
                    source.Orderings.Clear();
                    source.Orderings.Add(this.TranslateOrdering(methodCall, source));
                    return source;

                case nameof(Queryable.ThenBy) when arguments.Count == 2:
                case nameof(Queryable.ThenByDescending) when arguments.Count == 2:
                    source.EnsureNotPaged(methodCall);
                    source.Orderings.Add(this.TranslateOrdering(methodCall, source));
                    return source;

                case nameof(Queryable.Select) when arguments.Count == 2 && GetLambda(arguments[1]).Parameters.Count == 1:
                    // 投影不改變筆數，可以接在 Skip / Take 之後。
                    source.Selector = source.Compose(GetLambda(arguments[1]));
                    return source;

                case nameof(Queryable.Skip) when arguments.Count == 2:
                    if (source.Offset != null || source.Limit != null) throw NotTranslatable(methodCall);

                    source.Offset = this.TranslateCount(arguments[1], methodCall);
                    return source;

                case nameof(Queryable.Take) when arguments.Count == 2:
                    if (source.Limit != null) throw NotTranslatable(methodCall);

                    source.Limit = this.TranslateCount(arguments[1], methodCall);
                    return source;

                case nameof(Queryable.Distinct) when arguments.Count == 1:
                    source.EnsureNotPaged(methodCall);
                    source.IsDistinct = true;
                    return source;

                default: throw NotTranslatable(methodCall);
            }
        }

        private OrderingExpression TranslateOrdering(MethodCallExpression methodCall, QueryBuilder source)
        {
            var key = this.translator.Translate(source.Compose(GetLambda(methodCall.Arguments[1])), source.Table);

            // SQL Server 不能直接以條件排序，例如 OrderBy(x => x.Age > 18)。
            if (SqlExpressionTranslator.IsPredicate(key)) throw NotTranslatable(methodCall);

            var isAscending = methodCall.Method.Name == nameof(Queryable.OrderBy) || methodCall.Method.Name == nameof(Queryable.ThenBy);

            return new OrderingExpression(key, isAscending);
        }

        /// <summary>
        ///     經過 <see cref="PartialEvaluator" /> 後是參數（<c>__Skip_0</c> → <c>Skip_0</c>）；直接組出來、沒有經過的運算式則可能是常數，直接輸出數字。
        /// </summary>
        private SqlExpression TranslateCount(Expression count, MethodCallExpression methodCall)
        {
            switch (count)
            {
                case ConstantExpression constant:
                    return new SqlConstantExpression(constant.Value, typeof(int));

                case ParameterExpression parameter when this.context.TryGetParameterValue(parameter, out _):
                    return this.context.CreateParameter(parameter, typeof(int), null);

                default: throw NotTranslatable(methodCall);
            }
        }

        /// <summary>
        ///     翻譯過程中逐步累積的查詢內容，最後再建立不可變的 <see cref="SelectExpression" />。
        /// </summary>
        private sealed class QueryBuilder
        {
            public QueryBuilder(TableExpression table)
            {
                this.Table = table;
            }

            public TableExpression Table { get; }

            public SqlExpression Predicate { get; private set; }

            public List<OrderingExpression> Orderings { get; } = new List<OrderingExpression>();

            public SqlExpression Offset { get; set; }

            public SqlExpression Limit { get; set; }

            public bool IsDistinct { get; set; }

            /// <summary>
            ///     目前的投影，以實體為參數；還沒有 <c>Select</c> 時為 <c>null</c>，表示元素就是實體。
            /// </summary>
            public LambdaExpression Selector { get; set; }

            /// <summary>
            ///     把接在後面的 lambda 改寫成以實體為參數，例如 <c>Select</c> 之後的 <c>a =&gt; a.N</c> 改寫成 <c>x =&gt; x.Name</c>。
            /// </summary>
            public LambdaExpression Compose(LambdaExpression lambda)
            {
                return this.Selector == null ? lambda : LambdaComposer.Compose(lambda, this.Selector);
            }

            public void AddPredicate(SqlExpression predicate)
            {
                this.Predicate = this.Predicate == null ? predicate : new SqlBinaryExpression(SqlBinaryOperator.AndAlso, this.Predicate, predicate, typeof(bool));
            }

            /// <summary>
            ///     <c>Skip</c> / <c>Take</c> 之後再篩選、排序或去除重複，要先分頁再處理，需要子查詢。
            /// </summary>
            public void EnsureNotPaged(MethodCallExpression methodCall)
            {
                if (this.Offset != null || this.Limit != null)
                {
                    throw new NotSupportedException($"Can not translate '{methodCall}' to SQL: '{methodCall.Method.Name}' after Skip or Take is not supported yet.");
                }
            }
        }

        /// <summary>
        ///     依投影產生 <c>SELECT</c> 清單與 Shaper：
        ///     <list type="bullet">
        ///         <item>實體：選出所有可寫入的欄位，Shaper 是 <c>new Member { Id = Projection(0), ... }</c>。</item>
        ///         <item>匿名型別、DTO：逐一處理建構函式引數與成員指定，別名取自成員名稱。</item>
        ///         <item>條件（例如 <c>Adult = x.Age &gt;= 18</c>）：SQL Server 不能直接選出條件，輸出 <c>CASE WHEN ... THEN 1 ELSE 0 END</c>。</item>
        ///         <item>常數與查詢參數（例如 <c>Tag = tag</c>）不需要資料庫，原樣留在 Shaper，在用戶端取值。</item>
        ///     </list>
        ///     <para>別名重複時加上數字，例如 <c>Name</c>、<c>Name1</c>。</para>
        /// </summary>
        private sealed class ProjectionBuilder
        {
            private readonly SqlExpressionTranslator translator;
            private readonly TableExpression table;
            private readonly ParameterExpression parameter;
            private readonly List<ProjectionExpression> projections = new List<ProjectionExpression>();
            private readonly HashSet<string> aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            public ProjectionBuilder(SqlExpressionTranslator translator, TableExpression table, LambdaExpression selector)
            {
                this.translator = translator;
                this.table = table;

                if (selector == null)
                {
                    this.parameter = Expression.Parameter(table.Entity.ClrType, "x");
                    selector = Expression.Lambda(this.parameter, this.parameter);
                }
                else
                {
                    this.parameter = selector.Parameters[0];
                }

                this.Shaper = this.BuildShaper(selector.Body, null);

                // 投影完全不需要資料庫（例如 Select(x => 1)）時，仍然要選出一個欄位，SQL 才合法。
                if (this.projections.Count == 0) this.projections.Add(new ProjectionExpression(new SqlConstantExpression(1, typeof(int)), "Value"));
            }

            public IReadOnlyList<ProjectionExpression> Projections => this.projections;

            public Expression Shaper { get; }

            private Expression BuildShaper(Expression expression, string alias)
            {
                switch (expression)
                {
                    case ParameterExpression parameterExpr when parameterExpr == this.parameter:
                        return this.BuildEntityShaper(expression);

                    case NewExpression @new:
                        var parameters = @new.Constructor?.GetParameters();

                        return @new.Update(@new.Arguments.Select((a, i) => this.BuildShaper(a, @new.Members?[i].Name ?? parameters?[i].Name)));

                    case MemberInitExpression memberInit:
                        var bindings = memberInit.Bindings.Select(
                                b => b is MemberAssignment assignment
                                         ? (MemberBinding)assignment.Update(this.BuildShaper(assignment.Expression, assignment.Member.Name))
                                         : throw NotTranslatable(memberInit))
                            .ToArray();

                        return memberInit.Update((NewExpression)this.BuildShaper(memberInit.NewExpression, null), bindings);

                    case Expression value when IsClientValue(value):
                        return value;

                    default:
                        var sql = this.translator.Translate(Expression.Lambda(expression, this.parameter), this.table);

                        if (SqlExpressionTranslator.IsPredicate(sql))
                        {
                            sql = new CaseExpression(
                                new[] { new CaseWhenClause(sql, new SqlConstantExpression(true, typeof(bool))) },
                                new SqlConstantExpression(false, typeof(bool)),
                                typeof(bool));
                        }

                        return this.AddProjection(sql, alias ?? (sql is ColumnExpression column ? column.Property.PropertyName : "Value"), expression.Type);
                }
            }

            private Expression BuildEntityShaper(Expression entity)
            {
                var constructor = entity.Type.GetConstructor(Type.EmptyTypes);

                if (constructor == null) throw new NotSupportedException($"'{entity.Type}' must have a parameterless constructor to be materialized.");

                var bindings = this.table.Entity.Columns.Where(c => c.Property.CanWrite)
                    .Select(c => Expression.Bind(c.Property, this.AddProjection(this.table.Column(c), c.PropertyName, c.ClrType)));

                return Expression.MemberInit(Expression.New(constructor), bindings);
            }

            private ProjectionBindingExpression AddProjection(SqlExpression sql, string alias, Type type)
            {
                var uniqueAlias = alias;

                for (var i = 1; !this.aliases.Add(uniqueAlias); i++)
                {
                    uniqueAlias = string.Concat(alias, i.ToString());
                }

                this.projections.Add(new ProjectionExpression(sql, uniqueAlias));

                return new ProjectionBindingExpression(this.projections.Count - 1, type);
            }

            /// <summary>
            ///     不依賴欄位的值：常數，或 <see cref="PartialEvaluator" /> 產生的查詢參數（可能包著轉型）。
            /// </summary>
            private bool IsClientValue(Expression expression)
            {
                while (expression.NodeType == ExpressionType.Convert || expression.NodeType == ExpressionType.ConvertChecked)
                {
                    expression = ((UnaryExpression)expression).Operand;
                }

                return expression is ConstantExpression || (expression is ParameterExpression parameterExpr && parameterExpr != this.parameter);
            }
        }
    }
}
