using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
    ///         <item><c>Join</c> 見 <see cref="TranslateJoin" />；<c>GroupJoin</c> + <c>SelectMany</c>（Left Join）見 <see cref="TranslateGroupJoin" />；其他的 <c>SelectMany</c> 見 <see cref="TranslateCrossJoin" />。</item>
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
                new SelectExpression(projections, source.Sources, source.Predicate, source.Orderings, source.Offset, source.Limit, source.IsDistinct),
                shaper,
                resultKind,
                this.context.ParameterBindings.ToDictionary(b => b.Key, b => b.Value));
        }

        /// <summary>
        ///     依目前的投影（沒有 <c>Select</c> 時是實體）產生 <c>SELECT</c> 清單與 Shaper。
        /// </summary>
        private TranslatedQuery Build(QueryBuilder source, QueryResultKind resultKind)
        {
            var projection = new ProjectionBuilder(this.context, this.translator, source.Tables, source.OptionalTables, source.Selector);

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

                var predicate = this.translator.TranslatePredicate(source.Compose(GetLambda(methodCall.Arguments[1])), source.Tables);

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

        private static SqlExpression AndAlso(SqlExpression left, SqlExpression right)
        {
            if (left == null) return right;
            if (right == null) return left;

            return new SqlBinaryExpression(SqlBinaryOperator.AndAlso, left, right, typeof(bool));
        }

        /// <summary>
        ///     複合鍵 <c>new { m.A, m.B }</c> 拆成各個成員；其他的鍵就是單一值。
        /// </summary>
        private static IReadOnlyList<Expression> SplitKey(Expression key)
        {
            return key is NewExpression @new && @new.Members != null ? (IReadOnlyList<Expression>)@new.Arguments : new[] { key };
        }

        private QueryBuilder Visit(Expression expression)
        {
            switch (expression)
            {
                case ConstantExpression constant when constant.Value is IQueryable queryable:
                    return this.CreateRoot(queryable.ElementType, (queryable as IQueryRoot)?.TableName);

                case MethodCallExpression methodCall when methodCall.Method.DeclaringType == typeof(Queryable):
                    return this.VisitQueryableMethod(methodCall);

                default: throw NotTranslatable(expression);
            }
        }

        /// <summary>
        ///     <paramref name="tableName" /> 是指定的資料表名稱（例如 3.x 的 <c>factory.Create&lt;Member&gt;("tbl_member_archive")</c>），
        ///     只換掉名稱，schema 仍取自 <c>[Table]</c>。
        /// </summary>
        private QueryBuilder CreateRoot(Type elementType, string tableName)
        {
            var entity = EntityMetadata.For(elementType);
            var alias = CreateAlias(elementType, ++this.tableCount);

            return new QueryBuilder(tableName == null ? TableExpression.For(entity, alias) : new TableExpression(tableName, entity.Schema, alias, entity));
        }

        private QueryBuilder VisitQueryableMethod(MethodCallExpression methodCall)
        {
            var arguments = methodCall.Arguments;

            // GroupJoin 要和接在後面的 SelectMany 一起翻譯，不能先翻譯來源。
            if (methodCall.Method.Name == nameof(Queryable.SelectMany)
                && GetLambda(arguments[1]).Parameters.Count == 1
                && arguments[0] is MethodCallExpression groupJoin
                && groupJoin.Method.DeclaringType == typeof(Queryable)
                && groupJoin.Method.Name == nameof(Queryable.GroupJoin)
                && groupJoin.Arguments.Count == 5)
            {
                return this.TranslateGroupJoin(methodCall);
            }

            var source = this.Visit(arguments[0]);

            switch (methodCall.Method.Name)
            {
                case nameof(Queryable.Where) when arguments.Count == 2 && GetLambda(arguments[1]).Parameters.Count == 1:
                    source.EnsureNotPaged(methodCall);
                    source.AddPredicate(this.translator.TranslatePredicate(source.Compose(GetLambda(arguments[1])), source.Tables));
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

                case nameof(Queryable.Join) when arguments.Count == 5:
                    return this.TranslateJoin(source, methodCall);

                case nameof(Queryable.SelectMany) when GetLambda(arguments[1]).Parameters.Count == 1:
                    return this.TranslateCrossJoin(source, methodCall);

                default: throw NotTranslatable(methodCall);
            }
        }

        /// <summary>
        ///     <c>Join</c>（查詢語法的 <c>join … on … equals …</c>）翻譯成 <c>INNER JOIN</c>：
        ///     <list type="bullet">
        ///         <item>鍵以 SQL 的 <c>=</c> 比較，<c>NULL</c> 不等於任何值，與 LINQ to Objects 相同；複合鍵 <c>new { a, b } equals new { c, d }</c> 逐一比較，以 AND 結合。</item>
        ///         <item>被 Join 的一方的 <c>Where</c> 放進 <c>ON</c>，排序不影響結果，直接拿掉。</item>
        ///         <item>
        ///             結果選擇器（例如 <c>(m, d) =&gt; new { m, d }</c>）組成以所有資料表為參數的投影，之後的運算子照常對應回欄位，
        ///             所以可以一直 Join 下去，沒有 3.x 的 7 張表上限。
        ///         </item>
        ///     </list>
        ///     <para>
        ///         任一方已經分頁、<c>Distinct</c>，或被 Join 的一方本身已經有 Join 時需要子查詢，目前丟出 <see cref="NotSupportedException" />。
        ///     </para>
        /// </summary>
        private QueryBuilder TranslateJoin(QueryBuilder outer, MethodCallExpression methodCall)
        {
            var inner = this.Visit(methodCall.Arguments[1]);
            var outerSelector = outer.GetSelector();

            this.AddJoin(outer, inner, methodCall, JoinType.Inner);

            outer.Selector = LambdaComposer.Compose(GetLambda(methodCall.Arguments[4]), outerSelector, inner.GetSelector());

            return outer;
        }

        /// <summary>
        ///     查詢語法的 Left Join：<c>join d in departments on m.Id equals d.ManagerId into g from d in g.DefaultIfEmpty()</c>，
        ///     C# 編譯成 <c>GroupJoin(…, (m, g) =&gt; new { m, g }).SelectMany(t =&gt; t.g.DefaultIfEmpty(), (t, d) =&gt; …)</c>，翻譯成 <c>LEFT JOIN</c>；
        ///     沒有 <c>DefaultIfEmpty()</c>（<c>from d in g</c>）時只留下有對應的資料，翻譯成 <c>INNER JOIN</c>。
        ///     <list type="bullet">
        ///         <item>鍵、被 Join 一方的 <c>Where</c> 與 <see cref="TranslateJoin" /> 相同；<c>Where</c> 放在 <c>ON</c>，所以只篩選右邊，不會減少左邊的資料。</item>
        ///         <item>
        ///             右邊沒有資料時，選出的實體是 <c>null</c>；選出的欄位是 <c>NULL</c>，讀成 <c>default</c>（例如 <see cref="int" /> 是 0，與 Dapper 相同），
        ///             LINQ to Objects 則會丟出 <see cref="NullReferenceException" />。
        ///         </item>
        ///     </list>
        ///     <para>
        ///         用到群組本身（例如 <c>g.Count()</c>、<c>g.Where(…)</c>）或只有 <c>GroupJoin</c> 沒有 <c>SelectMany</c> 時，需要子查詢或 GroupBy，
        ///         目前丟出 <see cref="NotSupportedException" />。
        ///     </para>
        /// </summary>
        private QueryBuilder TranslateGroupJoin(MethodCallExpression selectMany)
        {
            var groupJoin = (MethodCallExpression)selectMany.Arguments[0];
            var outer = this.Visit(groupJoin.Arguments[0]);
            var inner = this.Visit(groupJoin.Arguments[1]);
            var outerSelector = outer.GetSelector();
            var innerSelector = inner.GetSelector();

            // (m, g) => new { m, g } 以左邊的資料表與群組 g 為參數。
            var groupSelector = GetLambda(groupJoin.Arguments[4]);
            var group = groupSelector.Parameters[1];
            var intermediate = LambdaComposer.Compose(groupSelector, outerSelector, Expression.Lambda(group, group));

            var joinType = GetGroupJoinType(LambdaComposer.Compose(GetLambda(selectMany.Arguments[1]), intermediate).Body, group)
                           ?? throw NotTranslatable(selectMany);

            // 結果選擇器可能還留著群組 g（例如中間有 where 時的 (t, d) => new { t, d }），之後的投影真的用到 g 時，翻譯器才丟出例外。
            var body = selectMany.Arguments.Count == 3
                           ? LambdaComposer.Compose(GetLambda(selectMany.Arguments[2]), intermediate, innerSelector).Body
                           : innerSelector.Body;

            this.AddJoin(outer, inner, groupJoin, joinType);

            outer.Selector = Expression.Lambda(body, outerSelector.Parameters.Concat(innerSelector.Parameters));

            return outer;
        }

        /// <summary>
        ///     不相關的 <c>SelectMany</c>（查詢語法連續兩個 <c>from</c>，例如 <c>from m in members from d in departments</c>）翻譯成 <c>CROSS JOIN</c>：
        ///     <list type="bullet">
        ///         <item>展開的集合是另一個查詢，它的 <c>Where</c> 放進 <c>WHERE</c>（CROSS JOIN 先篩選或後篩選結果相同），排序直接拿掉。</item>
        ///         <item>之後再加上 <c>where m.Id == d.ManagerId</c> 就是以 <c>WHERE</c> 結合的 Join。</item>
        ///     </list>
        ///     <para>
        ///         集合參考外層的資料（例如 <c>m =&gt; departments.Where(d =&gt; d.ManagerId == m.Id)</c>）、
        ///         任一方已經分頁或 <c>Distinct</c>、展開的查詢本身已經有 Join 時，目前丟出 <see cref="NotSupportedException" />。
        ///     </para>
        /// </summary>
        private QueryBuilder TranslateCrossJoin(QueryBuilder outer, MethodCallExpression selectMany)
        {
            var collection = GetLambda(selectMany.Arguments[1]);

            if (ParameterFinder.Contains(collection.Body, collection.Parameters[0]))
            {
                throw new NotSupportedException($"Can not translate '{selectMany}' to SQL: SelectMany whose collection depends on the outer element is not supported yet.");
            }

            var inner = this.Visit(collection.Body);

            if (outer.Offset != null || outer.Limit != null || outer.IsDistinct) throw NotTranslatable(selectMany);
            if (inner.Sources.Count > 1 || inner.Offset != null || inner.Limit != null || inner.IsDistinct) throw NotTranslatable(selectMany);

            var outerSelector = outer.GetSelector();
            var innerSelector = inner.GetSelector();

            outer.Sources.Add(new CrossJoinExpression(inner.Sources[0]));

            if (inner.Predicate != null) outer.AddPredicate(inner.Predicate);

            outer.Selector = selectMany.Arguments.Count == 3
                                 ? LambdaComposer.Compose(GetLambda(selectMany.Arguments[2]), outerSelector, innerSelector)
                                 : Expression.Lambda(innerSelector.Body, outerSelector.Parameters.Concat(innerSelector.Parameters));

            return outer;
        }

        /// <summary>
        ///     <c>SelectMany</c> 展開的集合：<c>g.DefaultIfEmpty()</c> 是 Left Join、<c>g</c> 是 Inner Join，其他（例如 <c>g.Where(…)</c>）無法翻譯，回傳 <c>null</c>。
        /// </summary>
        private static JoinType? GetGroupJoinType(Expression collection, ParameterExpression group)
        {
            if (collection == group) return JoinType.Inner;

            if (collection is MethodCallExpression methodCall
                && methodCall.Method.DeclaringType == typeof(Enumerable)
                && methodCall.Method.Name == nameof(Enumerable.DefaultIfEmpty)
                && methodCall.Arguments.Count == 1
                && methodCall.Arguments[0] == group)
            {
                return JoinType.Left;
            }

            return null;
        }

        /// <summary>
        ///     把 <paramref name="inner" /> 以 <paramref name="joinCall" />（<c>Join</c> 或 <c>GroupJoin</c>，第 3、4 個引數是鍵）的條件 Join 到 <paramref name="outer" />。
        ///     <para>
        ///         Left Join 時記下右邊的一個鍵欄位：有對應的資料時它一定不是 <c>NULL</c>（<c>NULL = …</c> 不成立），Shaper 以它判斷右邊的實體是否為 <c>null</c>。
        ///     </para>
        /// </summary>
        private void AddJoin(QueryBuilder outer, QueryBuilder inner, MethodCallExpression joinCall, JoinType joinType)
        {
            if (outer.Offset != null || outer.Limit != null || outer.IsDistinct) throw NotTranslatable(joinCall);
            if (inner.Sources.Count > 1 || inner.Offset != null || inner.Limit != null || inner.IsDistinct) throw NotTranslatable(joinCall);

            var outerTables = outer.Tables;
            var innerTable = inner.Tables[0];
            var outerKey = outer.Compose(GetLambda(joinCall.Arguments[2]));
            var innerKey = inner.Compose(GetLambda(joinCall.Arguments[3]));
            var outerParts = SplitKey(outerKey.Body);
            var innerParts = SplitKey(innerKey.Body);

            if (outerParts.Count != innerParts.Count) throw NotTranslatable(joinCall);

            SqlExpression predicate = null;
            ColumnExpression marker = null;

            for (var i = 0; i < outerParts.Count; i++)
            {
                var left = this.translator.Translate(Expression.Lambda(outerParts[i], outerKey.Parameters), outerTables);
                var right = this.translator.Translate(Expression.Lambda(innerParts[i], innerKey.Parameters), innerTable);

                if (SqlExpressionTranslator.IsPredicate(left) || SqlExpressionTranslator.IsPredicate(right)) throw NotTranslatable(joinCall);

                predicate = AndAlso(predicate, new SqlBinaryExpression(SqlBinaryOperator.Equal, left, right, typeof(bool)));
                marker = marker ?? right as ColumnExpression;
            }

            if (joinType == JoinType.Left)
            {
                // 右邊的鍵都不是欄位（例如 on m.Type equals 1）時，沒辦法分辨右邊有沒有資料。
                if (marker == null) throw new NotSupportedException($"Can not translate '{joinCall}' to SQL: the key of the joined source must contain a column.");

                outer.OptionalTables.Add(innerTable, marker);
            }

            outer.Sources.Add(new JoinExpression(joinType, inner.Sources[0], AndAlso(predicate, inner.Predicate)));
        }

        private OrderingExpression TranslateOrdering(MethodCallExpression methodCall, QueryBuilder source)
        {
            var key = this.translator.Translate(source.Compose(GetLambda(methodCall.Arguments[1])), source.Tables);

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

        private sealed class ParameterFinder : ExpressionVisitor
        {
            private readonly ParameterExpression parameter;
            private bool found;

            private ParameterFinder(ParameterExpression parameter)
            {
                this.parameter = parameter;
            }

            public static bool Contains(Expression expression, ParameterExpression parameter)
            {
                var finder = new ParameterFinder(parameter);

                finder.Visit(expression);

                return finder.found;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                this.found |= node == this.parameter;

                return node;
            }
        }

        /// <summary>
        ///     翻譯過程中逐步累積的查詢內容，最後再建立不可變的 <see cref="SelectExpression" />。
        /// </summary>
        private sealed class QueryBuilder
        {
            public QueryBuilder(TableExpression table)
            {
                this.Sources.Add(table);
            }

            /// <summary>
            ///     <c>FROM</c> 的來源：查詢的根，之後依序是 <see cref="JoinExpression" />。
            /// </summary>
            public List<TableExpressionBase> Sources { get; } = new List<TableExpressionBase>();

            /// <summary>
            ///     所有的資料表，依序對應 <see cref="Selector" /> 的參數。
            /// </summary>
            public TableExpression[] Tables =>
                this.Sources.Select(
                        s => (TableExpression)(s is JoinExpression join ? join.Table : s is CrossJoinExpression crossJoin ? crossJoin.Table : s))
                    .ToArray();

            /// <summary>
            ///     Left Join 的右邊可能沒有資料：資料表對應到一個有資料時一定不是 <c>NULL</c> 的欄位。
            /// </summary>
            public Dictionary<TableExpression, ColumnExpression> OptionalTables { get; } = new Dictionary<TableExpression, ColumnExpression>();

            public SqlExpression Predicate { get; private set; }

            public List<OrderingExpression> Orderings { get; } = new List<OrderingExpression>();

            public SqlExpression Offset { get; set; }

            public SqlExpression Limit { get; set; }

            public bool IsDistinct { get; set; }

            /// <summary>
            ///     目前的投影，依序以 <see cref="Tables" /> 的實體為參數；還沒有 <c>Select</c> 或 <c>Join</c> 時為 <c>null</c>，表示元素就是唯一一張資料表的實體。
            /// </summary>
            public LambdaExpression Selector { get; set; }

            /// <summary>
            ///     把接在後面的 lambda 改寫成以實體為參數，例如 <c>Select</c> 之後的 <c>a =&gt; a.N</c> 改寫成 <c>x =&gt; x.Name</c>。
            /// </summary>
            public LambdaExpression Compose(LambdaExpression lambda)
            {
                return this.Selector == null ? lambda : LambdaComposer.Compose(lambda, this.Selector);
            }

            /// <summary>
            ///     <see cref="Selector" />；還沒有投影時是 <c>x =&gt; x</c>。
            /// </summary>
            public LambdaExpression GetSelector()
            {
                if (this.Selector != null) return this.Selector;

                var parameter = Expression.Parameter(this.Tables[0].Entity.ClrType, "x");

                return Expression.Lambda(parameter, parameter);
            }

            public void AddPredicate(SqlExpression predicate)
            {
                this.Predicate = AndAlso(this.Predicate, predicate);
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
            private readonly SqlTranslationContext context;
            private readonly SqlExpressionTranslator translator;
            private readonly TableExpression[] tables;
            private readonly IReadOnlyDictionary<TableExpression, ColumnExpression> optionalTables;
            private readonly ReadOnlyCollection<ParameterExpression> parameters;
            private readonly List<ProjectionExpression> projections = new List<ProjectionExpression>();
            private readonly HashSet<string> aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            /// <param name="tables">依序對應 <paramref name="selector" /> 的參數。</param>
            /// <param name="optionalTables">Left Join 右邊的資料表，以及判斷有沒有資料的欄位。</param>
            /// <param name="selector">目前的投影；<c>null</c> 時元素就是唯一一張資料表的實體。</param>
            public ProjectionBuilder(
                SqlTranslationContext context,
                SqlExpressionTranslator translator,
                TableExpression[] tables,
                IReadOnlyDictionary<TableExpression, ColumnExpression> optionalTables,
                LambdaExpression selector)
            {
                this.context = context;
                this.translator = translator;
                this.tables = tables;
                this.optionalTables = optionalTables;

                if (selector == null)
                {
                    var parameter = Expression.Parameter(tables[0].Entity.ClrType, "x");
                    selector = Expression.Lambda(parameter, parameter);
                }

                this.parameters = selector.Parameters;
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
                    case ParameterExpression parameterExpr when this.parameters.Contains(parameterExpr):
                        return this.BuildEntityShaper(parameterExpr, this.tables[this.parameters.IndexOf(parameterExpr)]);

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
                        var sql = this.translator.Translate(Expression.Lambda(expression, this.parameters), this.tables);

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

            private Expression BuildEntityShaper(Expression entity, TableExpression table)
            {
                var constructor = entity.Type.GetConstructor(Type.EmptyTypes);

                if (constructor == null) throw new NotSupportedException($"'{entity.Type}' must have a parameterless constructor to be materialized.");

                var bindings = table.Entity.Columns.Where(c => c.Property.CanWrite)
                    .Select(c => Expression.Bind(c.Property, this.AddProjection(table.Column(c), c.PropertyName, c.ClrType)));

                var memberInit = Expression.MemberInit(Expression.New(constructor), bindings);

                if (!this.optionalTables.TryGetValue(table, out var marker)) return memberInit;

                // Left Join 的右邊沒有資料時是 null；判斷用的欄位已經在 SELECT 清單中就直接沿用。
                var index = this.projections.FindIndex(p => p.Expression is ColumnExpression column && column.Table == table && column.Name == marker.Name);

                if (index < 0) index = this.AddProjection(marker, marker.Property?.PropertyName ?? marker.Name, typeof(object)).Index;

                return Expression.Condition(
                    Expression.Equal(new ProjectionBindingExpression(index, typeof(object)), Expression.Constant(null)),
                    Expression.Constant(null, entity.Type),
                    memberInit);
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

                // 其他參數（例如 GroupJoin 的群組 g）交給翻譯器，丟出無法翻譯的例外。
                return expression is ConstantExpression || (expression is ParameterExpression parameterExpr && this.context.TryGetParameterValue(parameterExpr, out _));
            }
        }
    }
}
