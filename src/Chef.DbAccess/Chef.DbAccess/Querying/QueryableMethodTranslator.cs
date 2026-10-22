using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把 <see cref="Queryable" /> 運算子組成的查詢（已經過 <see cref="PartialEvaluator" />）翻譯成 <see cref="SelectExpression" />。
    ///     <list type="bullet">
    ///         <item>查詢的根是值為 <see cref="IQueryable" /> 的常數，依 <see cref="IQueryable.ElementType" /> 對應資料表，預設選出所有對應的欄位。</item>
    ///         <item>多次 <c>Where</c> 以 <c>AND</c> 結合（3.x 的 <c>Where</c> 會覆蓋前一個條件）。</item>
    ///         <item><c>OrderBy</c> 會取代先前的排序，<c>ThenBy</c> 接在後面，與 LINQ 的語意相同。</item>
    ///         <item>
    ///             <c>Skip</c> / <c>Take</c> 的數字由 <see cref="PartialEvaluator" /> 換成參數，SQL Server 輸出 <c>OFFSET {=Skip_0} ROWS</c>，
    ///             執行時由 Dapper 寫入數字，送到資料庫的 SQL 與 3.x 相同。
    ///         </item>
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

        public SelectExpression Translate(Expression query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            return this.Visit(query).Build();
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
            var table = TableExpression.For(entity, CreateAlias(elementType, ++this.tableCount));

            return new QueryBuilder(table, entity.Columns.Select(c => new ProjectionExpression(table.Column(c), c.PropertyName)));
        }

        private QueryBuilder VisitQueryableMethod(MethodCallExpression methodCall)
        {
            var source = this.Visit(methodCall.Arguments[0]);
            var arguments = methodCall.Arguments;

            switch (methodCall.Method.Name)
            {
                case nameof(Queryable.Where) when arguments.Count == 2 && GetLambda(arguments[1]).Parameters.Count == 1:
                    source.EnsureNotPaged(methodCall);
                    source.AddPredicate(this.translator.TranslatePredicate(GetLambda(arguments[1]), source.Table));
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
            var key = this.translator.Translate(GetLambda(methodCall.Arguments[1]), source.Table);

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

                case ParameterExpression parameter when this.context.TryGetParameterValue(parameter, out var value):
                    return this.context.CreateParameter(SqlExpressionTranslator.GetBaseName(parameter.Name), value, typeof(int), null);

                default: throw NotTranslatable(methodCall);
            }
        }

        private static NotSupportedException NotTranslatable(Expression expression)
        {
            return new NotSupportedException($"Can not translate '{expression}' to SQL.");
        }

        /// <summary>
        ///     翻譯過程中逐步累積的查詢內容，最後再建立不可變的 <see cref="SelectExpression" />。
        /// </summary>
        private sealed class QueryBuilder
        {
            private readonly IReadOnlyList<ProjectionExpression> projections;
            private SqlExpression predicate;

            public QueryBuilder(TableExpression table, IEnumerable<ProjectionExpression> projections)
            {
                this.Table = table;
                this.projections = projections.ToArray();
            }

            public TableExpression Table { get; }

            public List<OrderingExpression> Orderings { get; } = new List<OrderingExpression>();

            public SqlExpression Offset { get; set; }

            public SqlExpression Limit { get; set; }

            public bool IsDistinct { get; set; }

            public void AddPredicate(SqlExpression newPredicate)
            {
                this.predicate = this.predicate == null
                                     ? newPredicate
                                     : new SqlBinaryExpression(SqlBinaryOperator.AndAlso, this.predicate, newPredicate, typeof(bool));
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

            public SelectExpression Build()
            {
                return new SelectExpression(this.projections, new[] { this.Table }, this.predicate, this.Orderings, this.Offset, this.Limit, this.IsDistinct);
            }
        }
    }
}
