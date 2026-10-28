using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
        private IQueryRoot connectionRoot;

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

        /// <summary>
        ///     <c>Count</c>、<c>Any</c> 等不組出實體的結果用這個多載，一對多的 <c>Include</c> 不會 Join，不影響筆數。
        /// </summary>
        private TranslatedQuery Build(
            QueryBuilder source,
            IEnumerable<ProjectionExpression> projections,
            Expression shaper,
            QueryResultKind resultKind,
            IdentityResolver resolver = null)
        {
            return new TranslatedQuery(
                new SelectExpression(projections, source.Sources, source.Predicate, source.Orderings, source.Offset, source.Limit, source.IsDistinct),
                shaper,
                resultKind,
                this.context.ParameterBindings.ToDictionary(b => b.Key, b => b.Value),
                resolver);
        }

        /// <summary>
        ///     依目前的投影（沒有 <c>Select</c> 時是實體）產生 <c>SELECT</c> 清單與 Shaper。
        /// </summary>
        private TranslatedQuery Build(QueryBuilder source, QueryResultKind resultKind)
        {
            var resolver = this.PrepareCollectionIncludes(source, resultKind);
            var projection = new ProjectionBuilder(this.context, this.translator, source);

            // 一對多的 Join 在組出 Shaper 之後才加入，投影的參數不需要對應這些資料表。
            foreach (var include in source.CollectionIncludes)
            {
                source.Sources.Add(new JoinExpression(JoinType.Left, include.Table, include.Predicate));
            }

            return this.Build(source, projection.Projections, projection.Shaper, resultKind, resolver);
        }

        /// <summary>
        ///     一對多的 <c>Include</c> 只有查詢結果是實體本身時才能合併資料列（見 <see cref="IdentityResolver" />）；
        ///     <c>First</c>、<c>Single</c>、分頁與 <c>Distinct</c> 會套用在 Join 之後的資料列上，需要子查詢，目前不支援。
        /// </summary>
        /// <returns>沒有一對多的 <c>Include</c> 時為 <c>null</c>。</returns>
        private IdentityResolver PrepareCollectionIncludes(QueryBuilder source, QueryResultKind resultKind)
        {
            if (source.CollectionIncludes.Count == 0) return null;

            var owner = source.CollectionIncludes[0].Owner;
            var isEntity = source.Selector == null ? source.Tables[0] == owner : source.Selector.Body == source.Selector.Parameters[Array.IndexOf(source.Tables, owner)];

            if (resultKind != QueryResultKind.Sequence || source.Offset != null || source.Limit != null || source.IsDistinct)
            {
                throw new NotSupportedException("Include of a collection can not be combined with First, Single, Skip, Take or Distinct yet.");
            }

            if (!isEntity || source.CollectionIncludes.Any(i => i.Owner != owner))
            {
                throw new NotSupportedException($"Include of a collection requires the query to return '{owner.Entity.ClrType.Name}' itself.");
            }

            var keys = GetIdentityKeys(owner);
            var collections = source.CollectionIncludes.Select(i => (i.Navigation, GetIdentityKeys(i.Table))).ToList();

            foreach (var include in source.CollectionIncludes) source.OptionalTables.Add(include.Table, include.Marker);

            return new IdentityResolver(keys, collections);
        }

        private static IReadOnlyList<PropertyInfo> GetIdentityKeys(TableExpression table)
        {
            var keys = table.Entity.IdentityKeys;

            if (keys.Count == 0)
            {
                throw new NotSupportedException($"Include of a collection needs the keys of '{table.Entity.ClrType.Name}' to merge rows. Mark them with [Key].");
            }

            return keys.Select(k => k.Property).ToList();
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

                var lambda = this.Prepare(source, methodCall.Arguments[1]);
                var predicate = this.translator.TranslatePredicate(lambda, source.Tables);

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
                    return this.CreateRoot(queryable);

                case MethodCallExpression methodCall when methodCall.Method.DeclaringType == typeof(Queryable):
                    return this.VisitQueryableMethod(methodCall);

                case MethodCallExpression methodCall when methodCall.Method.DeclaringType == typeof(ChefQueryableExtensions)
                                                          && methodCall.Method.Name == nameof(ChefQueryableExtensions.Include):
                    return this.TranslateInclude(this.Visit(methodCall.Arguments[0]), methodCall);

                default: throw NotTranslatable(expression);
            }
        }

        /// <summary>
        ///     查詢的根對應到資料表：
        ///     <list type="bullet">
        ///         <item>指定的資料表名稱（例如 3.x 的 <c>factory.Create&lt;Member&gt;("tbl_member_archive")</c>）只換掉名稱，schema 仍取自 <c>[Table]</c>。</item>
        ///         <item>
        ///             第一個根是執行查詢的連線，Join 的其他來源要在同一台伺服器，否則丟出 <see cref="InvalidOperationException" />；
        ///             在其他資料庫時，資料表名稱加上資料庫，例如 <c>[Member].[dbo].[Department]</c>，與 3.x 相同。
        ///             伺服器與資料庫不分大小寫比較，不知道時（例如不是 <c>Query()</c> 建立的根）視為相同。
        ///         </item>
        ///     </list>
        /// </summary>
        private QueryBuilder CreateRoot(IQueryable queryable)
        {
            var root = queryable as IQueryRoot;
            var elementType = queryable.ElementType;
            var entity = EntityMetadata.For(elementType);
            var alias = CreateAlias(elementType, ++this.tableCount);
            string database = null;

            if (this.tableCount == 1)
            {
                this.connectionRoot = root;
            }
            else if (this.connectionRoot?.Server != null && root?.Server != null)
            {
                if (!string.Equals(this.connectionRoot.Server, root.Server, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"Can not join '{elementType.Name}': it is on database server '{root.Server}', but the query runs on '{this.connectionRoot.Server}'.");
                }

                if (this.connectionRoot.Database != null && root.Database != null && !string.Equals(this.connectionRoot.Database, root.Database, StringComparison.OrdinalIgnoreCase))
                {
                    database = root.Database;
                }
            }

            return new QueryBuilder(new TableExpression(root?.TableName ?? entity.TableName, entity.Schema, alias, entity, database));
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
                    var predicate = this.Prepare(source, arguments[1]);
                    source.AddPredicate(this.translator.TranslatePredicate(predicate, source.Tables));
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
                    source.Selector = this.Prepare(source, arguments[1]);
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

            this.AddJoin(outer, inner, methodCall, JoinType.Inner);

            // AddJoin 可能因為鍵用到導覽屬性而加入資料表，之後才取外側的投影。
            outer.Selector = this.ExpandNavigations(outer, LambdaComposer.Compose(GetLambda(methodCall.Arguments[4]), outer.GetSelector(), inner.GetSelector()));

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

            // (m, g) => new { m, g } 以左邊的資料表與群組 g 為參數。
            var groupSelector = GetLambda(groupJoin.Arguments[4]);
            var group = groupSelector.Parameters[1];
            var collection = LambdaComposer.Compose(GetLambda(selectMany.Arguments[1]), groupSelector).Body;

            var joinType = GetGroupJoinType(collection, group) ?? throw NotTranslatable(selectMany);

            this.AddJoin(outer, inner, groupJoin, joinType);

            // AddJoin 可能因為鍵用到導覽屬性而加入資料表，之後才取外側的投影。
            var outerSelector = outer.GetSelector();
            var innerSelector = inner.GetSelector();
            var intermediate = LambdaComposer.Compose(groupSelector, outerSelector, Expression.Lambda(group, group));

            // 結果選擇器可能還留著群組 g（例如中間有 where 時的 (t, d) => new { t, d }），之後的投影真的用到 g 時，翻譯器才丟出例外。
            var body = selectMany.Arguments.Count == 3
                           ? LambdaComposer.Compose(GetLambda(selectMany.Arguments[2]), intermediate, innerSelector).Body
                           : innerSelector.Body;

            outer.Selector = this.ExpandNavigations(outer, Expression.Lambda(body, outerSelector.Parameters.Concat(innerSelector.Parameters)));

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

            outer.Selector = this.ExpandNavigations(
                outer,
                selectMany.Arguments.Count == 3
                    ? LambdaComposer.Compose(GetLambda(selectMany.Arguments[2]), outerSelector, innerSelector)
                    : Expression.Lambda(innerSelector.Body, outerSelector.Parameters.Concat(innerSelector.Parameters)));

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

            // 外側的鍵可以用導覽屬性（例如 m.Department.ManagerId），展開後才取資料表。
            var outerKey = this.Prepare(outer, joinCall.Arguments[2]);
            var innerKey = inner.Compose(GetLambda(joinCall.Arguments[3]));
            var outerTables = outer.Tables;
            var innerTable = inner.Tables[0];
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

        /// <summary>
        ///     運算子的 lambda 改寫成以所有資料表為參數（<see cref="QueryBuilder.Compose" />），再展開導覽屬性（<see cref="ExpandNavigations" />）。
        /// </summary>
        private LambdaExpression Prepare(QueryBuilder source, Expression argument)
        {
            return this.ExpandNavigations(source, source.Compose(GetLambda(argument)));
        }

        /// <summary>
        ///     把 lambda 中以 <c>[ForeignKey]</c> 設定的導覽屬性換成 Join 進來的資料表，例如 <c>x =&gt; x.Department.Name</c> 的 <c>x.Department</c>
        ///     換成 <c>LEFT JOIN [tbl_department] [dep_2] ON [mem_1].[DepartmentId] = [dep_2].[Id]</c> 的參數；可以連續導覽，例如 <c>x.Department.Manager.Name</c>。
        ///     <list type="bullet">
        ///         <item>
        ///             一律用 <c>LEFT JOIN</c>：資料庫不一定有外鍵約束，<c>INNER JOIN</c> 會讓找不到對應資料的列消失。
        ///             多對一的 Join 不會增加筆數，所以可以接在 <c>Skip</c> / <c>Take</c> 之後。
        ///         </item>
        ///         <item>同一張資料表的同一個導覽屬性只 Join 一次，之後的運算子共用；被 Join 的資料表與來源在同一個資料庫。</item>
        ///         <item>選出導覽屬性本身（例如 <c>new { x.Name, x.Department }</c>）時，沒有對應的資料就是 <c>null</c>。</item>
        ///     </list>
        /// </summary>
        /// <param name="lambda">參數依序對應 <paramref name="source" /> 的資料表。</param>
        /// <returns>參數依序對應展開後所有資料表的 lambda。</returns>
        private LambdaExpression ExpandNavigations(QueryBuilder source, LambdaExpression lambda)
        {
            var expander = new NavigationExpander(this, source, lambda.Parameters);
            var body = expander.Visit(lambda.Body);

            return Expression.Lambda(body, source.Tables.Select(expander.GetParameter));
        }

        private TableExpression JoinNavigation(QueryBuilder source, TableExpression table, NavigationMetadata navigation)
        {
            if (source.Navigations.TryGetValue((table, navigation.Property), out var joined)) return joined;

            var principalKeys = navigation.PrincipalKeys;

            joined = this.CreateNavigationTable(table, navigation.TargetType);

            SqlExpression predicate = null;

            for (var i = 0; i < principalKeys.Count; i++)
            {
                predicate = AndAlso(
                    predicate,
                    new SqlBinaryExpression(SqlBinaryOperator.Equal, table.Column(navigation.ForeignKeys[i]), joined.Column(principalKeys[i]), typeof(bool)));
            }

            AddNavigationJoin(source, table, navigation.Property, joined, predicate, joined.Column(principalKeys[0]));

            return joined;
        }

        /// <summary>
        ///     導覽屬性指到的資料表，與來源在同一個資料庫。
        /// </summary>
        private TableExpression CreateNavigationTable(TableExpression table, Type entityType)
        {
            var target = EntityMetadata.For(entityType);

            return new TableExpression(target.TableName, target.Schema, CreateAlias(target.ClrType, ++this.tableCount), target, table.Database);
        }

        /// <param name="marker">有對應的資料時一定不是 <c>NULL</c> 的欄位，見 <see cref="QueryBuilder.OptionalTables" />。</param>
        private static void AddNavigationJoin(QueryBuilder source, TableExpression table, PropertyInfo property, TableExpression joined, SqlExpression predicate, ColumnExpression marker)
        {
            var selector = source.GetSelector();

            source.Sources.Add(new JoinExpression(JoinType.Left, joined, predicate));
            source.OptionalTables.Add(joined, marker);
            source.Navigations.Add((table, property), joined);

            // 投影的參數要一直對應所有的資料表。
            source.Selector = Expression.Lambda(selector.Body, selector.Parameters.Concat(new[] { Expression.Parameter(joined.Entity.ClrType, property.Name) }));
        }

        /// <summary>
        ///     <c>Include</c>（見 <see cref="ChefQueryableExtensions" />）：Join 導覽屬性指到的資料表，並記錄在 <see cref="QueryBuilder.Includes" />，
        ///     組出實體時一併填入導覽屬性。
        ///     <list type="bullet">
        ///         <item><c>Include(x =&gt; x.Department.Manager)</c> 依序 Join 以 <c>[ForeignKey]</c> 設定的每一層導覽屬性，與 <see cref="ExpandNavigations" /> 共用 Join。</item>
        ///         <item>
        ///             <c>Include(x =&gt; x.Department, (x, d) =&gt; x.DepartmentId == d.Id)</c> 以條件 <c>LEFT JOIN</c>，只能有一層。
        ///             條件中要有被 Join 資料表的欄位與其他值相等（例如 <c>d.Id</c>），用來判斷有沒有對應的資料；同一個導覽屬性已經 Join 過時不能再指定條件。
        ///         </item>
        ///         <item>
        ///             一對多的集合只能以條件 Include，例如 <c>Include(x =&gt; x.ManagedDepartments, (x, d) =&gt; x.Id == d.ManagerId)</c>，
        ///             記錄在 <see cref="QueryBuilder.CollectionIncludes" />，見 <see cref="AddCollectionInclude" /> 與 <see cref="PrepareCollectionIncludes" />。
        ///         </item>
        ///     </list>
        ///     <para>導覽屬性要屬於查詢中的實體（例如 <c>Select</c> 成匿名型別之後就不行），否則丟出 <see cref="NotSupportedException" />。</para>
        /// </summary>
        private QueryBuilder TranslateInclude(QueryBuilder source, MethodCallExpression methodCall)
        {
            var navigation = source.Compose(GetLambda(methodCall.Arguments[1]));
            var members = new Stack<MemberExpression>();

            // 集合的 Include 是 Func<T, IEnumerable<TElement>>，屬性外面可能包著轉型。
            var expression = navigation.Body.NodeType == ExpressionType.Convert ? ((UnaryExpression)navigation.Body).Operand : navigation.Body;

            while (expression is MemberExpression member && member.Member is PropertyInfo)
            {
                members.Push(member);
                expression = member.Expression;
            }

            // 之後的引數依參數個數區分：條件是 (x, d) => ...，欄位是 d => new { ... }。
            var lambdas = methodCall.Arguments.Skip(2).Select(GetLambda).ToList();
            var conditionLambda = lambdas.FirstOrDefault(l => l.Parameters.Count == 2);
            var columnsLambda = lambdas.FirstOrDefault(l => l.Parameters.Count == 1);

            var index = expression is ParameterExpression parameter ? navigation.Parameters.IndexOf(parameter) : -1;

            if (index < 0 || members.Count == 0 || (conditionLambda != null && members.Count > 1))
            {
                throw new NotSupportedException($"Can not translate '{methodCall}' to SQL: Include must specify a navigation property of the entity, for example 'x => x.Department'.");
            }

            var table = source.Tables[index];

            if (conditionLambda != null)
            {
                var property = (PropertyInfo)members.Pop().Member;

                // 條件的第二個參數是被 Join 的實體；與屬性的型別不同時，屬性是它的集合。
                var entityType = conditionLambda.Parameters[1].Type;
                var columns = ParseIncludeColumns(columnsLambda, entityType, methodCall);
                var isCollection = property.PropertyType != entityType;

                if (source.Navigations.ContainsKey((table, property)) || source.CollectionIncludes.Any(i => i.Owner == table && i.Navigation == property))
                {
                    throw new NotSupportedException($"Can not translate '{methodCall}' to SQL: '{property.Name}' is already joined.");
                }

                var joined = this.CreateNavigationTable(table, entityType);
                var target = Expression.Parameter(entityType, property.Name);
                var condition = LambdaComposer.Compose(conditionLambda, source.GetSelector(), Expression.Lambda(target, target));
                var predicate = this.translator.TranslatePredicate(condition, source.Tables.Concat(new[] { joined }).ToArray());
                var marker = FindEqualColumn(predicate, joined)
                             ?? throw new NotSupportedException($"Can not translate '{methodCall}' to SQL: the condition must compare a column of '{entityType.Name}' for equality.");

                if (isCollection)
                {
                    AddCollectionInclude(source, table, property, joined, predicate, marker, columns);

                    return source;
                }

                AddNavigationJoin(source, table, property, joined, predicate, marker);
                source.AddInclude(table, property, joined, columns);

                return source;
            }

            var path = members.ToList();

            foreach (var member in path)
            {
                // 集合（List<T> 等，string 除外）只能以條件 Include。
                if (member.Type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(member.Type))
                {
                    throw new NotSupportedException(
                        $"Can not translate '{methodCall}' to SQL: Include of a collection must specify the join condition, " +
                        $"for example 'Include(x => x.{member.Member.Name}, (x, y) => x.Id == y.ParentId)'.");
                }

                var metadata = table.Entity.FindNavigation(member.Member.Name)
                               ?? throw new NotSupportedException(
                                   $"Can not translate '{methodCall}' to SQL: '{member}' must be configured with [ForeignKey], or specify the join condition.");

                var joined = this.JoinNavigation(source, table, metadata);

                // 連續導覽時，欄位只限制最後一層。
                var columns = member == path[path.Count - 1] ? ParseIncludeColumns(columnsLambda, metadata.TargetType, methodCall) : null;

                source.AddInclude(table, metadata.Property, joined, columns);
                table = joined;
            }

            return source;
        }

        /// <summary>
        ///     一對多的 Join 會增加筆數，記錄在 <see cref="QueryBuilder.CollectionIncludes" />，等到產生 SELECT 清單時才加入（見 <see cref="Build(QueryBuilder, QueryResultKind)" />），
        ///     所以 <c>Count</c> 等不受影響。集合屬性要有 setter，並且能指定 <see cref="List{T}" />。
        ///     <para>只取部分欄位時，合併資料列需要的子資料鍵也會一併選出並填入。</para>
        /// </summary>
        private static void AddCollectionInclude(
            QueryBuilder source,
            TableExpression owner,
            PropertyInfo property,
            TableExpression children,
            SqlExpression predicate,
            ColumnExpression marker,
            HashSet<PropertyMetadata> columns)
        {
            var elementType = children.Entity.ClrType;

            if (!property.CanWrite || !property.PropertyType.IsAssignableFrom(typeof(List<>).MakeGenericType(elementType)))
            {
                throw new NotSupportedException($"Can not include '{property.Name}': the collection property must have a setter and accept List<{elementType.Name}>.");
            }

            columns?.UnionWith(children.Entity.IdentityKeys);

            source.CollectionIncludes.Add((owner, property, children, predicate, marker, columns));
        }

        /// <summary>
        ///     <c>Include</c> 指定的欄位，例如 <c>d =&gt; new { d.Id, d.Name }</c> 或 <c>d =&gt; d.Name</c>，只能是 <paramref name="entityType" /> 對應到欄位的屬性。
        /// </summary>
        /// <returns>沒有指定時為 <c>null</c>，表示所有欄位。</returns>
        private static HashSet<PropertyMetadata> ParseIncludeColumns(LambdaExpression columns, Type entityType, MethodCallExpression methodCall)
        {
            if (columns == null) return null;

            var entity = EntityMetadata.For(entityType);
            var parts = columns.Body is NewExpression @new && @new.Members != null ? (IEnumerable<Expression>)@new.Arguments : new[] { columns.Body };
            var result = new HashSet<PropertyMetadata>();

            foreach (var part in parts)
            {
                var member = (part.NodeType == ExpressionType.Convert ? ((UnaryExpression)part).Operand : part) as MemberExpression;
                var column = member?.Expression == columns.Parameters[0] ? entity.FindColumn(member.Member.Name) : null;

                if (column == null)
                {
                    throw new NotSupportedException(
                        $"Can not translate '{methodCall}' to SQL: the columns of Include must be mapped properties of '{entityType.Name}', for example 'd => new {{ d.Id, d.Name }}'.");
                }

                result.Add(column);
            }

            return result;
        }

        /// <summary>
        ///     <c>AND</c> 串起來的條件中，與其他值比較是否相等的 <paramref name="table" /> 的欄位，例如 <c>[mem_1].[No] = [dep_2].[ManagerId]</c> 的 <c>[dep_2].[ManagerId]</c>。
        /// </summary>
        private static ColumnExpression FindEqualColumn(SqlExpression predicate, TableExpression table)
        {
            if (!(predicate is SqlBinaryExpression binary)) return null;

            switch (binary.Operator)
            {
                case SqlBinaryOperator.AndAlso:
                    return FindEqualColumn(binary.Left, table) ?? FindEqualColumn(binary.Right, table);

                case SqlBinaryOperator.Equal:
                    return new[] { binary.Left, binary.Right }.OfType<ColumnExpression>().FirstOrDefault(c => c.Table == table);

                default: return null;
            }
        }

        private OrderingExpression TranslateOrdering(MethodCallExpression methodCall, QueryBuilder source)
        {
            var lambda = this.Prepare(source, methodCall.Arguments[1]);
            var key = this.translator.Translate(lambda, source.Tables);

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
        ///     見 <see cref="ExpandNavigations" />。lambda 的參數依序對應資料表；Join 進來的資料表各自建立新的參數。
        /// </summary>
        private sealed class NavigationExpander : ExpressionVisitor
        {
            private readonly QueryableMethodTranslator owner;
            private readonly QueryBuilder source;
            private readonly Dictionary<TableExpression, ParameterExpression> parameters = new Dictionary<TableExpression, ParameterExpression>();
            private readonly Dictionary<ParameterExpression, TableExpression> tables = new Dictionary<ParameterExpression, TableExpression>();

            public NavigationExpander(QueryableMethodTranslator owner, QueryBuilder source, IReadOnlyList<ParameterExpression> lambdaParameters)
            {
                this.owner = owner;
                this.source = source;

                var sourceTables = source.Tables;

                for (var i = 0; i < lambdaParameters.Count; i++)
                {
                    this.parameters.Add(sourceTables[i], lambdaParameters[i]);
                    this.tables.Add(lambdaParameters[i], sourceTables[i]);
                }
            }

            public ParameterExpression GetParameter(TableExpression table)
            {
                if (this.parameters.TryGetValue(table, out var parameter)) return parameter;

                parameter = Expression.Parameter(table.Entity.ClrType, table.Alias);

                this.parameters.Add(table, parameter);
                this.tables.Add(parameter, table);

                return parameter;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                var instance = this.Visit(node.Expression);

                if (instance is ParameterExpression parameter && this.tables.TryGetValue(parameter, out var table) && node.Member is PropertyInfo property)
                {
                    // 已經 Join 過（包含 Include 以條件 Join、沒有 [ForeignKey] 的導覽屬性）就沿用。
                    if (this.source.Navigations.TryGetValue((table, property), out var joined)) return this.GetParameter(joined);

                    if (table.Entity.FindNavigation(property.Name) is NavigationMetadata navigation)
                    {
                        return this.GetParameter(this.owner.JoinNavigation(this.source, table, navigation));
                    }
                }

                return node.Update(instance);
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

            /// <summary>
            ///     已經 Join 進來的導覽屬性：同一張資料表的同一個導覽屬性只 Join 一次。
            /// </summary>
            public Dictionary<(TableExpression Table, PropertyInfo Navigation), TableExpression> Navigations { get; } =
                new Dictionary<(TableExpression Table, PropertyInfo Navigation), TableExpression>();

            /// <summary>
            ///     <c>Include</c> 的導覽屬性：組出資料表的實體時，一併以 Join 進來的資料表填入導覽屬性。
            ///     <c>Columns</c> 是只取部分欄位時的欄位，<c>null</c> 表示所有欄位。
            /// </summary>
            public Dictionary<TableExpression, List<(PropertyInfo Navigation, TableExpression Table, HashSet<PropertyMetadata> Columns)>> Includes { get; } =
                new Dictionary<TableExpression, List<(PropertyInfo Navigation, TableExpression Table, HashSet<PropertyMetadata> Columns)>>();

            /// <summary>
            ///     一對多集合的 <c>Include</c>：<c>Owner</c> 的實體以 <c>LEFT JOIN</c> <c>Table</c> 的資料填入集合 <c>Navigation</c>。
            ///     Join 等到產生 <c>SELECT</c> 清單時才加入，<c>Marker</c> 用來判斷有沒有對應的資料；<c>Columns</c> 同 <see cref="Includes" />。
            /// </summary>
            public List<(TableExpression Owner, PropertyInfo Navigation, TableExpression Table, SqlExpression Predicate, ColumnExpression Marker,
                HashSet<PropertyMetadata> Columns)> CollectionIncludes { get; } =
                new List<(TableExpression Owner, PropertyInfo Navigation, TableExpression Table, SqlExpression Predicate, ColumnExpression Marker,
                    HashSet<PropertyMetadata> Columns)>();

            /// <summary>
            ///     同一個導覽屬性 Include 多次時，欄位取聯集；任何一次沒有指定欄位就是所有欄位。
            /// </summary>
            public void AddInclude(TableExpression table, PropertyInfo navigation, TableExpression joined, HashSet<PropertyMetadata> columns)
            {
                if (!this.Includes.TryGetValue(table, out var includes))
                {
                    this.Includes.Add(table, includes = new List<(PropertyInfo Navigation, TableExpression Table, HashSet<PropertyMetadata> Columns)>());
                }

                var index = includes.FindIndex(i => i.Navigation == navigation && i.Table == joined);

                if (index < 0)
                {
                    includes.Add((navigation, joined, columns));
                }
                else if (includes[index].Columns != null)
                {
                    if (columns == null) includes[index] = (navigation, joined, null);
                    else includes[index].Columns.UnionWith(columns);
                }
            }

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
            private static readonly MethodInfo CreateCollectionMethod = typeof(IdentityResolver).GetMethod(nameof(IdentityResolver.CreateCollection));

            private readonly SqlTranslationContext context;
            private readonly SqlExpressionTranslator translator;
            private readonly TableExpression[] tables;
            private readonly QueryBuilder source;
            private readonly ReadOnlyCollection<ParameterExpression> parameters;
            private readonly List<ProjectionExpression> projections = new List<ProjectionExpression>();
            private readonly HashSet<string> aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            /// <param name="source">
            ///     依 <see cref="QueryBuilder.Selector" />（<c>null</c> 時元素就是唯一一張資料表的實體）產生，
            ///     並用到 Left Join 右邊的資料表（<see cref="QueryBuilder.OptionalTables" />）與 <see cref="QueryBuilder.Includes" />。
            /// </param>
            public ProjectionBuilder(SqlTranslationContext context, SqlExpressionTranslator translator, QueryBuilder source)
            {
                this.context = context;
                this.translator = translator;
                this.source = source;
                this.tables = source.Tables;

                var selector = source.Selector;

                if (selector == null)
                {
                    var parameter = Expression.Parameter(this.tables[0].Entity.ClrType, "x");
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
                        return this.BuildEntityShaper(parameterExpr.Type, this.tables[this.parameters.IndexOf(parameterExpr)]);

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

            /// <summary>
            ///     <c>new Member { Id = Projection(0), ..., Department = ... }</c>：先選出所有可寫入的欄位（<paramref name="columns" /> 不是 <c>null</c> 時只選出這些欄位），
            ///     再以同樣方式組出 <c>Include</c> 的導覽屬性。判斷 Left Join 有沒有資料的欄位不在 <paramref name="columns" /> 中時仍會選出，但不填入屬性。
            /// </summary>
            private Expression BuildEntityShaper(Type type, TableExpression table, HashSet<PropertyMetadata> columns = null)
            {
                var constructor = type.GetConstructor(Type.EmptyTypes);

                if (constructor == null) throw new NotSupportedException($"'{type}' must have a parameterless constructor to be materialized.");

                var bindings = table.Entity.Columns.Where(c => c.Property.CanWrite && (columns == null || columns.Contains(c)))
                    .Select(c => Expression.Bind(c.Property, this.AddProjection(table.Column(c), c.PropertyName, c.ClrType)))
                    .ToList();

                if (this.source.Includes.TryGetValue(table, out var includes))
                {
                    foreach (var (navigation, joined, includeColumns) in includes)
                    {
                        if (!navigation.CanWrite) throw new NotSupportedException($"Can not include '{type.Name}.{navigation.Name}': the property must have a setter.");

                        bindings.Add(Expression.Bind(navigation, this.BuildEntityShaper(navigation.PropertyType, joined, includeColumns)));
                    }
                }

                // 一對多：每列的集合只有 0 或 1 筆子資料，之後由 IdentityResolver 合併。
                foreach (var include in this.source.CollectionIncludes.Where(i => i.Owner == table))
                {
                    var elementType = include.Table.Entity.ClrType;
                    var child = this.BuildEntityShaper(elementType, include.Table, include.Columns);

                    bindings.Add(Expression.Bind(include.Navigation, Expression.Call(CreateCollectionMethod.MakeGenericMethod(elementType), child)));
                }

                var memberInit = Expression.MemberInit(Expression.New(constructor), bindings);

                if (!this.source.OptionalTables.TryGetValue(table, out var marker)) return memberInit;

                // Left Join 的右邊沒有資料時是 null；判斷用的欄位已經在 SELECT 清單中就直接沿用。
                var index = this.projections.FindIndex(p => p.Expression is ColumnExpression column && column.Table == table && column.Name == marker.Name);

                if (index < 0) index = this.AddProjection(marker, marker.Property?.PropertyName ?? marker.Name, typeof(object)).Index;

                return Expression.Condition(
                    Expression.Equal(new ProjectionBindingExpression(index, typeof(object)), Expression.Constant(null)),
                    Expression.Constant(null, type),
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
