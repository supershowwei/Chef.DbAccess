using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把 lambda（例如 <c>Where</c> 的條件）翻譯成 SQL AST。lambda 參數依位置對應到資料表，成員存取對應到欄位。
    ///     <list type="bullet">
    ///         <item>比較、<c>&amp;&amp;</c>、<c>||</c>、<c>!</c>、算術與位元運算；常數與閉包變數一律變成參數，並帶上比較的欄位，讓方言決定參數型別。</item>
    ///         <item>和 <c>null</c> 比較輸出 <c>IS NULL</c> / <c>IS NOT NULL</c>，閉包變數的值是 <c>null</c> 時也一樣。</item>
    ///         <item>條件中單獨的布林欄位 <c>x.IsActive</c> 輸出 <c>[IsActive] = {=IsActive_0}</c>，<c>!x.IsActive</c> 的值為 <c>false</c>（與 3.x 相同）。</item>
    ///         <item>列舉、<c>Nullable&lt;T&gt;</c> 與數值之間的轉型直接沿用欄位，不輸出 <c>CAST</c>。</item>
    ///         <item><c>?:</c> 輸出 <c>CASE WHEN</c>（條件中的布林分支改寫成 <c>AND</c> / <c>OR</c>）、<c>??</c> 輸出 <c>COALESCE</c>。</item>
    ///         <item>
    ///             <c>HasValue</c> / <c>Value</c>、<c>Equals</c>、<c>string.IsNullOrEmpty</c>，以及 <c>string</c> 的
    ///             <c>Contains</c> / <c>StartsWith</c> / <c>EndsWith</c>（<c>LIKE</c>）。
    ///         </item>
    ///         <item>其他成員與方法（<c>x.Name.Length</c>、<c>x.Name.ToUpper()</c>……）交給方言提供的 <see cref="IMemberTranslator" /> 與 <see cref="IMethodCallTranslator" />。</item>
    ///     </list>
    ///     <para>無法翻譯的運算式一律丟出 <see cref="NotSupportedException" />，並附上該段運算式，不在用戶端偷偷計算。</para>
    /// </summary>
    internal sealed class SqlExpressionTranslator
    {
        private static readonly HashSet<Type> NumericTypes = new HashSet<Type>
                                                             {
                                                                 typeof(byte),
                                                                 typeof(sbyte),
                                                                 typeof(short),
                                                                 typeof(ushort),
                                                                 typeof(int),
                                                                 typeof(uint),
                                                                 typeof(long),
                                                                 typeof(ulong),
                                                                 typeof(float),
                                                                 typeof(double),
                                                                 typeof(decimal)
                                                             };

        private static readonly SqlConstantExpression Wildcard = new SqlConstantExpression("%", typeof(string));

        private readonly SqlTranslationContext context;
        private readonly IReadOnlyList<IMemberTranslator> memberTranslators;
        private readonly IReadOnlyList<IMethodCallTranslator> methodCallTranslators;
        private readonly Dictionary<ParameterExpression, TableExpression> tables = new Dictionary<ParameterExpression, TableExpression>();
        private LambdaExpression lambda;

        /// <param name="context">同一個查詢共用的參數值與參數命名。</param>
        /// <param name="memberTranslators">方言提供的成員翻譯器，依序嘗試。</param>
        /// <param name="methodCallTranslators">方言提供的方法翻譯器，依序嘗試。</param>
        public SqlExpressionTranslator(
            SqlTranslationContext context,
            IEnumerable<IMemberTranslator> memberTranslators = null,
            IEnumerable<IMethodCallTranslator> methodCallTranslators = null)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.memberTranslators = memberTranslators?.ToArray() ?? Array.Empty<IMemberTranslator>();
            this.methodCallTranslators = methodCallTranslators?.ToArray() ?? Array.Empty<IMethodCallTranslator>();
        }

        /// <summary>
        ///     翻譯成條件（<c>WHERE</c>、<c>ON</c>），例如 <c>x =&gt; x.Age &gt; 30</c>。
        /// </summary>
        public SqlExpression TranslatePredicate(LambdaExpression lambda, params TableExpression[] tables)
        {
            this.Bind(lambda, tables);

            return this.TranslatePredicate(lambda.Body);
        }

        /// <summary>
        ///     翻譯成值（例如排序的鍵），例如 <c>x =&gt; x.Age</c>。
        /// </summary>
        public SqlExpression Translate(LambdaExpression lambda, params TableExpression[] tables)
        {
            this.Bind(lambda, tables);

            return this.Translate(lambda.Body, null);
        }

        internal static bool IsPredicate(SqlExpression expression)
        {
            switch (expression)
            {
                case SqlBinaryExpression binary:
                    switch (binary.Operator)
                    {
                        case SqlBinaryOperator.Equal:
                        case SqlBinaryOperator.NotEqual:
                        case SqlBinaryOperator.LessThan:
                        case SqlBinaryOperator.LessThanOrEqual:
                        case SqlBinaryOperator.GreaterThan:
                        case SqlBinaryOperator.GreaterThanOrEqual:
                        case SqlBinaryOperator.AndAlso:
                        case SqlBinaryOperator.OrElse:
                            return true;

                        default: return false;
                    }

                case SqlUnaryExpression unary: return unary.Operator != SqlUnaryOperator.Negate;
                case InExpression _: return true;
                case LikeExpression _: return true;

                // 回傳布林的函式是 SQL 的述詞，例如 SQL Server 全文檢索的 CONTAINS(...)。
                case SqlFunctionExpression function: return function.Type == typeof(bool);

                default: return false;
            }
        }

        /// <summary>
        ///     找出運算式裡第一個欄位，讓另一邊的值可以依這個欄位決定參數名稱與型別，例如 <c>(x.Id &amp; 1) &gt; 2</c> 的 <c>2</c> 對應到 <c>Id</c>。
        ///     <para>
        ///         函式只在回傳型別與欄位相同時才沿用欄位，例如 <c>UPPER([Name]) = @Name_0</c> 沿用（<c>varchar</c> 欄位一樣送成 DbString），
        ///         <c>LEN([Name]) &gt; 3</c> 不沿用。
        ///     </para>
        /// </summary>
        private static PropertyMetadata FindProperty(SqlExpression expression)
        {
            switch (expression)
            {
                case ColumnExpression column: return column.Property;
                case SqlBinaryExpression binary: return FindProperty(binary.Left) ?? FindProperty(binary.Right);
                case SqlUnaryExpression unary: return FindProperty(unary.Operand);

                case SqlFunctionExpression function:
                    var property = function.Arguments.Select(FindProperty).FirstOrDefault(p => p != null);

                    return property != null && UnwrapType(property.ClrType) == UnwrapType(function.Type) ? property : null;

                case CaseExpression @case:
                    return @case.WhenClauses.Select(w => FindProperty(w.Result)).FirstOrDefault(p => p != null)
                           ?? (@case.ElseResult == null ? null : FindProperty(@case.ElseResult));

                default: return null;
            }
        }

        private static Expression StripConversion(Expression expression)
        {
            while (expression is UnaryExpression unary && (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
            {
                expression = unary.Operand;
            }

            return expression;
        }

        /// <summary>
        ///     <c>Equals(object)</c> 的參數會先轉成 <see cref="object" />，去掉這層轉型才能比較型別。
        /// </summary>
        private static Expression StripObjectConversion(Expression expression)
        {
            return expression.NodeType == ExpressionType.Convert && expression.Type == typeof(object) ? ((UnaryExpression)expression).Operand : expression;
        }

        /// <summary>
        ///     列舉與底層型別、<c>T</c> 與 <c>Nullable&lt;T&gt;</c>、數值與數值之間的轉型不需要在 SQL 中處理。
        ///     <para>
        ///         <see cref="decimal" /> 的轉型會帶著 <c>op_Implicit</c> / <c>op_Explicit</c> 方法（例如 <c>x.Price &gt; x.Age</c>），一樣視為數值之間的轉型；
        ///         其他帶方法的轉型（例如 <c>DateTime</c> 轉 <c>DateTimeOffset</c>）不翻譯。
        ///     </para>
        /// </summary>
        private static bool IsTransparentConversion(UnaryExpression conversion)
        {
            var from = UnwrapType(conversion.Operand.Type);
            var to = UnwrapType(conversion.Type);

            if (NumericTypes.Contains(from) && NumericTypes.Contains(to)) return true;

            return conversion.Method == null && from == to;
        }

        private static Type UnwrapType(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;

            return type.IsEnum ? Enum.GetUnderlyingType(type) : type;
        }

        private void Bind(LambdaExpression lambda, TableExpression[] tables)
        {
            if (lambda == null) throw new ArgumentNullException(nameof(lambda));
            if (tables == null) throw new ArgumentNullException(nameof(tables));
            if (lambda.Parameters.Count != tables.Length) throw new ArgumentException("Each lambda parameter must have a table.", nameof(tables));
            if (tables.Any(t => t?.Entity == null)) throw new ArgumentException("Tables must be entity tables.", nameof(tables));

            this.lambda = lambda;
            this.tables.Clear();

            for (var i = 0; i < tables.Length; i++)
            {
                this.tables.Add(lambda.Parameters[i], tables[i]);
            }
        }

        private SqlExpression TranslatePredicate(Expression expression)
        {
            // x => true、x => false
            if (StripConversion(expression) is ConstantExpression constant && constant.Value is bool boolean)
            {
                return new SqlBinaryExpression(
                    SqlBinaryOperator.Equal,
                    new SqlConstantExpression(1, typeof(int)),
                    new SqlConstantExpression(boolean ? 1 : 0, typeof(int)),
                    typeof(bool));
            }

            // x => x.IsVip ? x.Age > 18 : x.Age > 21：SQL Server 的 CASE 不能回傳條件，改寫成 (test AND a) OR (NOT (test) AND b)。
            if (expression is ConditionalExpression conditional)
            {
                var test = this.TranslatePredicate(conditional.Test);

                return new SqlBinaryExpression(
                    SqlBinaryOperator.OrElse,
                    new SqlBinaryExpression(SqlBinaryOperator.AndAlso, test, this.TranslatePredicate(conditional.IfTrue), typeof(bool)),
                    new SqlBinaryExpression(
                        SqlBinaryOperator.AndAlso,
                        new SqlUnaryExpression(SqlUnaryOperator.Not, test, typeof(bool)),
                        this.TranslatePredicate(conditional.IfFalse),
                        typeof(bool)),
                    typeof(bool));
            }

            return this.ToPredicate(this.Translate(expression, null), true, expression);
        }

        /// <summary>
        ///     把值轉成條件：布林欄位變成 <c>[IsActive] = {=IsActive_0}</c>、布林參數變成 <c>{=flag_0} = 1</c>；
        ///     <paramref name="value" /> 為 <c>false</c> 時是否定的條件。
        /// </summary>
        private SqlExpression ToPredicate(SqlExpression sql, bool value, Expression source)
        {
            // !x.Level.HasValue 直接輸出 IS NULL，不輸出 NOT (... IS NOT NULL)。
            if (!value && sql is SqlUnaryExpression unary && (unary.Operator == SqlUnaryOperator.IsNull || unary.Operator == SqlUnaryOperator.IsNotNull))
            {
                var nullCheck = unary.Operator == SqlUnaryOperator.IsNull ? SqlUnaryOperator.IsNotNull : SqlUnaryOperator.IsNull;

                return new SqlUnaryExpression(nullCheck, unary.Operand, typeof(bool));
            }

            // !ids.Contains(x.Id) 直接輸出 NOT IN。
            if (!value && sql is InExpression @in) return new InExpression(@in.Item, @in.Values, !@in.IsNegated);

            if (IsPredicate(sql)) return value ? sql : new SqlUnaryExpression(SqlUnaryOperator.Not, sql, typeof(bool));

            switch (sql)
            {
                case ColumnExpression column when UnwrapType(column.Type) == typeof(bool):
                    return new SqlBinaryExpression(
                        SqlBinaryOperator.Equal,
                        column,
                        this.context.CreateParameter(null, value, typeof(bool), column.Property),
                        typeof(bool));

                case SqlParameterExpression parameter when UnwrapType(parameter.Type) == typeof(bool):
                    return new SqlBinaryExpression(SqlBinaryOperator.Equal, parameter, new SqlConstantExpression(value, typeof(bool)), typeof(bool));

                default: throw this.NotTranslatable(source);
            }
        }

        private SqlExpression Translate(Expression expression, PropertyMetadata hint)
        {
            switch (expression)
            {
                case BinaryExpression binary: return this.TranslateBinary(binary, hint);
                case UnaryExpression unary: return this.TranslateUnary(unary, hint);
                case MemberExpression member: return this.TranslateMember(member, hint);
                case MethodCallExpression methodCall: return this.TranslateMethodCall(methodCall, hint);
                case ConstantExpression constant: return this.context.CreateParameter(null, constant.Value, constant.Type, hint);
                case ParameterExpression parameter: return this.TranslateParameter(parameter, hint);
                case ConditionalExpression conditional: return this.TranslateConditional(conditional, hint);
                default: throw this.NotTranslatable(expression);
            }
        }

        /// <summary>
        ///     <c>x.Age &gt; 18 ? x.Name : "minor"</c> 輸出 <c>CASE WHEN [Age] &gt; {=Age_0} THEN [Name] ELSE @Name_0 END</c>，
        ///     值的那一邊沿用另一邊的欄位決定參數型別。回傳條件的分支（例如 <c>? x.Age &gt; 18 : ...</c>）只能放在條件中。
        /// </summary>
        private SqlExpression TranslateConditional(ConditionalExpression conditional, PropertyMetadata hint)
        {
            var test = this.TranslatePredicate(conditional.Test);
            var (ifTrue, ifFalse) = this.TranslatePair(conditional.IfTrue, conditional.IfFalse, hint);

            if (IsPredicate(ifTrue) || IsPredicate(ifFalse)) throw this.NotTranslatable(conditional);

            return new CaseExpression(new[] { new CaseWhenClause(test, ifTrue) }, ifFalse, conditional.Type);
        }

        /// <summary>
        ///     <c>x.Name ?? "-"</c> 輸出 <c>COALESCE([Name], @Name_0)</c>，<c>x.Level ?? 0</c> 輸出 <c>COALESCE([Level], {=Level_0})</c>。
        /// </summary>
        private SqlExpression TranslateCoalesce(BinaryExpression coalesce, PropertyMetadata hint)
        {
            if (coalesce.Conversion != null) throw this.NotTranslatable(coalesce);

            var (left, right) = this.TranslatePair(coalesce.Left, coalesce.Right, hint);

            return new SqlFunctionExpression("COALESCE", new[] { left, right }, coalesce.Type);
        }

        private SqlExpression TranslateBinary(BinaryExpression binary, PropertyMetadata hint)
        {
            var isBoolean = UnwrapType(binary.Type) == typeof(bool);

            switch (binary.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.And when isBoolean:
                    return new SqlBinaryExpression(SqlBinaryOperator.AndAlso, this.TranslatePredicate(binary.Left), this.TranslatePredicate(binary.Right), typeof(bool));

                case ExpressionType.OrElse:
                case ExpressionType.Or when isBoolean:
                    return new SqlBinaryExpression(SqlBinaryOperator.OrElse, this.TranslatePredicate(binary.Left), this.TranslatePredicate(binary.Right), typeof(bool));

                case ExpressionType.Equal: return this.TranslateEquality(binary.Left, binary.Right, false, hint);
                case ExpressionType.NotEqual: return this.TranslateEquality(binary.Left, binary.Right, true, hint);
                case ExpressionType.Coalesce: return this.TranslateCoalesce(binary, hint);
            }

            var (@operator, isComparison) = this.GetOperator(binary, isBoolean);
            var (left, right) = this.TranslatePair(binary.Left, binary.Right, hint);

            return new SqlBinaryExpression(@operator, left, right, isComparison ? typeof(bool) : binary.Type);
        }

        /// <summary>
        ///     <c>==</c>、<c>!=</c> 與 <c>Equals</c>；其中一邊是 <c>null</c> 時輸出 <c>IS NULL</c> / <c>IS NOT NULL</c>。
        /// </summary>
        private SqlExpression TranslateEquality(Expression left, Expression right, bool isNegated, PropertyMetadata hint)
        {
            var nullCheck = isNegated ? SqlUnaryOperator.IsNotNull : SqlUnaryOperator.IsNull;

            if (this.IsNullValue(right)) return new SqlUnaryExpression(nullCheck, this.Translate(left, hint), typeof(bool));
            if (this.IsNullValue(left)) return new SqlUnaryExpression(nullCheck, this.Translate(right, hint), typeof(bool));

            var (sqlLeft, sqlRight) = this.TranslatePair(left, right, hint);

            return new SqlBinaryExpression(isNegated ? SqlBinaryOperator.NotEqual : SqlBinaryOperator.Equal, sqlLeft, sqlRight, typeof(bool));
        }

        /// <summary>
        ///     先翻譯含欄位的一邊，值的那一邊才能依欄位決定參數名稱與型別，例如 <c>30 &lt; x.Age</c>、<c>"J".Contains(x.Name)</c>。
        /// </summary>
        private (SqlExpression Left, SqlExpression Right) TranslatePair(Expression left, Expression right, PropertyMetadata hint)
        {
            if (this.IsValue(left) && !this.IsValue(right))
            {
                var sqlRight = this.Translate(right, hint);

                return (this.Translate(left, FindProperty(sqlRight) ?? hint), sqlRight);
            }

            var sqlLeft = this.Translate(left, hint);

            return (sqlLeft, this.Translate(right, FindProperty(sqlLeft) ?? hint));
        }

        private (SqlBinaryOperator Operator, bool IsComparison) GetOperator(BinaryExpression binary, bool isBoolean)
        {
            switch (binary.NodeType)
            {
                case ExpressionType.LessThan: return (SqlBinaryOperator.LessThan, true);
                case ExpressionType.LessThanOrEqual: return (SqlBinaryOperator.LessThanOrEqual, true);
                case ExpressionType.GreaterThan: return (SqlBinaryOperator.GreaterThan, true);
                case ExpressionType.GreaterThanOrEqual: return (SqlBinaryOperator.GreaterThanOrEqual, true);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    return (SqlBinaryOperator.Add, false);
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return (SqlBinaryOperator.Subtract, false);
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return (SqlBinaryOperator.Multiply, false);
                case ExpressionType.Divide: return (SqlBinaryOperator.Divide, false);
                case ExpressionType.Modulo: return (SqlBinaryOperator.Modulo, false);
                case ExpressionType.And: return (SqlBinaryOperator.BitwiseAnd, false);
                case ExpressionType.Or: return (SqlBinaryOperator.BitwiseOr, false);
                case ExpressionType.ExclusiveOr when !isBoolean: return (SqlBinaryOperator.BitwiseXor, false);
                default: throw this.NotTranslatable(binary);
            }
        }

        private SqlExpression TranslateUnary(UnaryExpression unary, PropertyMetadata hint)
        {
            switch (unary.NodeType)
            {
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    if (!IsTransparentConversion(unary)) throw this.NotTranslatable(unary);

                    return this.Translate(unary.Operand, hint);

                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    return new SqlUnaryExpression(SqlUnaryOperator.Negate, this.Translate(unary.Operand, hint), unary.Type);

                case ExpressionType.UnaryPlus:
                    return this.Translate(unary.Operand, hint);

                case ExpressionType.Not when UnwrapType(unary.Type) == typeof(bool):
                    return this.ToPredicate(this.Translate(unary.Operand, hint), false, unary);

                default: throw this.NotTranslatable(unary);
            }
        }

        private SqlExpression TranslateMember(MemberExpression member, PropertyMetadata hint)
        {
            if (member.Expression is ParameterExpression parameter && this.tables.TryGetValue(parameter, out var table))
            {
                var property = table.Entity.FindColumn(member.Member.Name);

                if (property == null) throw new NotSupportedException($"'{member}' is not mapped to a column. ({this.lambda})");

                return table.Column(property);
            }

            // x.Level.HasValue、x.Level.Value
            if (member.Expression != null && Nullable.GetUnderlyingType(member.Expression.Type) != null)
            {
                switch (member.Member.Name)
                {
                    case nameof(Nullable<int>.HasValue):
                        return new SqlUnaryExpression(SqlUnaryOperator.IsNotNull, this.Translate(member.Expression, hint), typeof(bool));

                    case nameof(Nullable<int>.Value):
                        return this.Translate(member.Expression, hint);
                }
            }

            // x.Name.Length 等交給方言。靜態成員（DateTime.Now）已經由 PartialEvaluator 在用戶端求值。
            if (member.Expression != null)
            {
                var instance = this.Translate(member.Expression, null);
                var returnType = (member.Member as PropertyInfo)?.PropertyType ?? ((FieldInfo)member.Member).FieldType;

                foreach (var translator in this.memberTranslators)
                {
                    var translated = translator.Translate(instance, member.Member, returnType);

                    if (translated != null) return translated;
                }
            }

            throw this.NotTranslatable(member);
        }

        private SqlExpression TranslateMethodCall(MethodCallExpression methodCall, PropertyMetadata hint)
        {
            var method = methodCall.Method;
            var arguments = methodCall.Arguments;

            if (method.Name == nameof(object.Equals) && method.ReturnType == typeof(bool))
            {
                // x.Id.Equals(1)、x.Status.Equals(status)（參數會先轉成 object）、string.Equals(x.Name, name)
                var (left, right) = methodCall.Object != null && arguments.Count == 1
                                        ? (methodCall.Object, StripObjectConversion(arguments[0]))
                                        : method.DeclaringType == typeof(string) && method.IsStatic && arguments.Count == 2
                                            ? (arguments[0], arguments[1])
                                            : (null, null);

                // PartialEvaluator 會把 Convert(MemberStatus.Active, object) 摺疊成型別為 object 的常數，這種值也接受。
                if (left != null && (UnwrapType(left.Type) == UnwrapType(right.Type) || (right.Type == typeof(object) && this.IsValue(right))))
                {
                    return this.TranslateEquality(left, right, false, hint);
                }
            }

            if (method.DeclaringType == typeof(string) && methodCall.Object != null && arguments.Count == 1 && arguments[0].Type == typeof(string))
            {
                switch (method.Name)
                {
                    case nameof(string.Contains): return this.TranslateLike(methodCall, true, true, hint);
                    case nameof(string.StartsWith): return this.TranslateLike(methodCall, false, true, hint);
                    case nameof(string.EndsWith): return this.TranslateLike(methodCall, true, false, hint);
                }
            }

            if (method.DeclaringType == typeof(string) && method.Name == nameof(string.IsNullOrEmpty) && arguments.Count == 1)
            {
                return this.TranslateIsNullOrEmpty(arguments[0]);
            }

            if (this.TryGetCollectionContains(methodCall, out var collection, out var item))
            {
                return this.TranslateCollectionContains(collection, item);
            }

            // 其他方法交給方言；引數含有 lambda（例如 Enumerable.Any）時這裡不處理。
            if (methodCall.Arguments.All(a => a.NodeType != ExpressionType.Lambda && a.NodeType != ExpressionType.Quote))
            {
                var parts = this.TranslateAll(methodCall.Object == null ? arguments : new[] { methodCall.Object }.Concat(arguments).ToArray(), hint);
                var instance = methodCall.Object == null ? null : parts[0];
                var sqlArguments = methodCall.Object == null ? parts : parts.Skip(1).ToArray();

                foreach (var translator in this.methodCallTranslators)
                {
                    var translated = translator.Translate(instance, method, sqlArguments);

                    if (translated != null) return translated;
                }
            }

            throw this.NotTranslatable(methodCall);
        }

        /// <summary>
        ///     和 <see cref="TranslatePair" /> 一樣先翻譯含欄位的部分，值再依第一個欄位決定參數名稱與型別；
        ///     但只有型別相同的值才沿用欄位，例如 <c>x.Name.Substring(1, 2)</c> 的 1、2 不會變成 <c>Name_0</c>。
        /// </summary>
        private IReadOnlyList<SqlExpression> TranslateAll(IReadOnlyList<Expression> expressions, PropertyMetadata hint)
        {
            var translated = new SqlExpression[expressions.Count];

            for (var i = 0; i < expressions.Count; i++)
            {
                if (!this.IsValue(expressions[i])) translated[i] = this.Translate(expressions[i], hint);
            }

            var property = translated.Where(t => t != null).Select(FindProperty).FirstOrDefault(p => p != null) ?? hint;

            for (var i = 0; i < expressions.Count; i++)
            {
                if (translated[i] != null) continue;

                var valueHint = property != null && UnwrapType(property.ClrType) == UnwrapType(expressions[i].Type) ? property : null;

                translated[i] = this.Translate(expressions[i], valueHint);
            }

            return translated;
        }

        /// <summary>
        ///     集合（常數或閉包變數）的 <c>Contains</c>：<c>Enumerable.Contains(ids, x.Id)</c>、<c>list.Contains(x.Id)</c>，
        ///     以及 C# 14 的 <c>MemoryExtensions.Contains(ReadOnlySpan&lt;int&gt;.op_Implicit(ids), x.Id)</c>。
        /// </summary>
        private bool TryGetCollectionContains(MethodCallExpression methodCall, out Expression collection, out Expression item)
        {
            collection = item = null;

            if (methodCall.Method.Name != nameof(Enumerable.Contains) || methodCall.Method.ReturnType != typeof(bool)) return false;

            if (methodCall.Object == null && methodCall.Arguments.Count == 2)
            {
                collection = StripSpanConversion(methodCall.Arguments[0]);
                item = methodCall.Arguments[1];
            }
            else if (methodCall.Object != null && methodCall.Arguments.Count == 1)
            {
                collection = methodCall.Object;
                item = methodCall.Arguments[0];
            }
            else
            {
                return false;
            }

            return collection.Type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(collection.Type) && this.IsValue(collection);
        }

        /// <summary>
        ///     C# 14 把陣列轉成 <c>ReadOnlySpan&lt;T&gt;</c> / <c>Span&lt;T&gt;</c> 的 <c>op_Implicit</c>，PartialEvaluator 不會對 span 求值，裡面的陣列已經是參數。
        /// </summary>
        private static Expression StripSpanConversion(Expression expression)
        {
            var (method, operand) = expression is MethodCallExpression call && call.Arguments.Count == 1
                                        ? (call.Method, call.Arguments[0])
                                        : expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert
                                            ? (unary.Method, unary.Operand)
                                            : (null, null);

            if (method == null || method.Name != "op_Implicit" || !method.DeclaringType.IsGenericType) return expression;

            var definition = method.DeclaringType.GetGenericTypeDefinition();

            return definition.FullName == "System.ReadOnlySpan`1" || definition.FullName == "System.Span`1" ? operand : expression;
        }

        /// <summary>
        ///     輸出 <c>[Id] IN ({=Id_0}, {=Id_1})</c>，每個元素是一個參數並帶上欄位（3.x 是以 <c>OR</c> 串起 <c>=</c>）。
        ///     <list type="bullet">
        ///         <item>空集合（或集合本身是 <c>null</c>）輸出 <c>1 = 0</c>，<c>NOT</c> 時是 <c>1 = 1</c>（與 3.x 相同）。</item>
        ///         <item>元素有 <c>null</c> 時另外加上 <c>OR [Name] IS NULL</c>，因為 <c>IN (NULL)</c> 比對不到 <c>NULL</c>。</item>
        ///     </list>
        ///     <para>SQL 依元素個數與是否有 <c>null</c> 而不同，所以 <see cref="QueryCacheKey" /> 也記錄這兩項。</para>
        /// </summary>
        private SqlExpression TranslateCollectionContains(Expression collection, Expression item)
        {
            var sqlItem = this.Translate(item, null);
            var property = FindProperty(sqlItem);
            var queryParameter = StripConversion(collection) as ParameterExpression;
            var baseName = queryParameter == null ? null : SqlTranslationContext.GetBaseName(queryParameter.Name);
            var elements = (IEnumerable)this.GetValue(collection) ?? Array.Empty<object>();

            var values = new List<SqlExpression>();
            var hasNull = false;

            foreach (var element in elements)
            {
                if (element == null)
                {
                    hasNull = true;
                }
                else if (queryParameter == null)
                {
                    values.Add(this.context.CreateParameter(baseName, element, item.Type, property));
                }
                else
                {
                    // 下次執行時取新集合的第 i 個非 null 元素；QueryCacheKey 保證元素個數與是否有 null 相同。
                    var name = queryParameter.Name;
                    var index = values.Count;

                    values.Add(
                        this.context.CreateParameter(
                            baseName,
                            element,
                            item.Type,
                            property,
                            parameterValues => QueryParameterValues.GetNonNullElements(parameterValues, name)[index]));
                }
            }

            var isNull = new SqlUnaryExpression(SqlUnaryOperator.IsNull, sqlItem, typeof(bool));

            if (!hasNull) return new InExpression(sqlItem, values);
            if (values.Count == 0) return isNull;

            return new SqlBinaryExpression(SqlBinaryOperator.OrElse, new InExpression(sqlItem, values), isNull, typeof(bool));
        }

        /// <summary>
        ///     <c>string.IsNullOrEmpty(x.Name)</c> 輸出 <c>[Name] IS NULL OR [Name] LIKE ''</c>。
        ///     不用 <c>= ''</c>：SQL Server 比較字串時會忽略結尾空白，<c>'  ' = ''</c> 為真，但 C# 的 <c>IsNullOrEmpty("  ")</c> 為假。
        /// </summary>
        private SqlExpression TranslateIsNullOrEmpty(Expression argument)
        {
            var value = this.Translate(argument, null);

            return new SqlBinaryExpression(
                SqlBinaryOperator.OrElse,
                new SqlUnaryExpression(SqlUnaryOperator.IsNull, value, typeof(bool)),
                new LikeExpression(value, new SqlConstantExpression(string.Empty, typeof(string))),
                typeof(bool));
        }

        /// <summary>
        ///     <c>x.Name.Contains(keyword)</c> 輸出 <c>[Name] LIKE '%' + @Name_0 + '%'</c>，<c>StartsWith</c>、<c>EndsWith</c> 只在一邊加上 <c>%</c>。
        ///     <para>
        ///         值裡有 <c>%</c>、<c>_</c>、<c>[</c> 或 <c>\</c> 時才跳脫並加上 <c>ESCAPE '\'</c>，和 C# 一樣只比對字面上的文字（3.x 不跳脫）；
        ///         沒有時輸出與 3.x 相同（見 <see cref="LikePattern" />）。
        ///         欄位等其他運算式無法事先知道值，一律在 SQL 中以 <c>REPLACE</c> 跳脫並加上 <c>ESCAPE</c>。
        ///     </para>
        /// </summary>
        private SqlExpression TranslateLike(MethodCallExpression methodCall, bool leadingWildcard, bool trailingWildcard, PropertyMetadata hint)
        {
            var (instance, pattern) = this.TranslatePair(methodCall.Object, methodCall.Arguments[0], hint);

            char? escapeChar = null;

            if (pattern is SqlParameterExpression parameter)
            {
                var value = (string)parameter.Value;

                if (LikePattern.NeedsEscape(value))
                {
                    pattern = this.context.TransformParameter(parameter, v => LikePattern.Escape((string)v));
                    escapeChar = LikePattern.EscapeChar;
                }
            }
            else
            {
                pattern = EscapeInSql(pattern);
                escapeChar = LikePattern.EscapeChar;
            }

            if (leadingWildcard) pattern = new SqlBinaryExpression(SqlBinaryOperator.Add, Wildcard, pattern, typeof(string));
            if (trailingWildcard) pattern = new SqlBinaryExpression(SqlBinaryOperator.Add, pattern, Wildcard, typeof(string));

            return new LikeExpression(instance, pattern, escapeChar);
        }

        private static SqlExpression EscapeInSql(SqlExpression pattern)
        {
            foreach (var specialChar in LikePattern.SpecialChars)
            {
                pattern = new SqlFunctionExpression(
                    "REPLACE",
                    new SqlExpression[]
                    {
                        pattern,
                        new SqlConstantExpression(specialChar.ToString(), typeof(string)),
                        new SqlConstantExpression(string.Concat(LikePattern.EscapeChar.ToString(), specialChar.ToString()), typeof(string))
                    },
                    typeof(string));
            }

            return pattern;
        }

        private SqlExpression TranslateParameter(ParameterExpression parameter, PropertyMetadata hint)
        {
            if (this.context.TryGetParameterValue(parameter, out _)) return this.context.CreateParameter(parameter, parameter.Type, hint);

            throw this.NotTranslatable(parameter);
        }

        /// <summary>
        ///     常數或查詢參數（不含欄位的值）。
        /// </summary>
        private bool IsValue(Expression expression)
        {
            expression = StripConversion(expression);

            return expression is ConstantExpression || (expression is ParameterExpression parameter && !this.tables.ContainsKey(parameter));
        }

        /// <summary>
        ///     取得常數或查詢參數這次執行的值（呼叫前要先用 <see cref="IsValue" /> 確認）。
        /// </summary>
        private object GetValue(Expression expression)
        {
            switch (StripConversion(expression))
            {
                case ConstantExpression constant: return constant.Value;
                case ParameterExpression parameter when this.context.TryGetParameterValue(parameter, out var value): return value;
                default: throw this.NotTranslatable(expression);
            }
        }

        private bool IsNullValue(Expression expression)
        {
            switch (StripConversion(expression))
            {
                case ConstantExpression constant: return constant.Value == null;
                case ParameterExpression parameter: return this.context.TryGetParameterValue(parameter, out var value) && value == null;
                default: return false;
            }
        }

        private NotSupportedException NotTranslatable(Expression expression)
        {
            return new NotSupportedException(
                ReferenceEquals(expression, this.lambda?.Body)
                    ? $"Can not translate '{this.lambda}' to SQL."
                    : $"Can not translate '{expression}' in '{this.lambda}' to SQL.");
        }
    }
}
