using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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
    ///     </list>
    ///     <para>
    ///         無法翻譯的運算式一律丟出 <see cref="NotSupportedException" />，並附上該段運算式，不在用戶端偷偷計算。
    ///         <c>string</c>、<c>DateTime</c>、<c>Math</c> 等成員與方法之後由翻譯器擴充。
    ///     </para>
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

        private readonly SqlTranslationContext context;
        private readonly Dictionary<ParameterExpression, TableExpression> tables = new Dictionary<ParameterExpression, TableExpression>();
        private LambdaExpression lambda;

        public SqlExpressionTranslator(SqlTranslationContext context)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
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

        private static bool IsPredicate(SqlExpression expression)
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
                        case SqlBinaryOperator.Like:
                            return true;

                        default: return false;
                    }

                case SqlUnaryExpression unary: return unary.Operator != SqlUnaryOperator.Negate;
                case InExpression _: return true;
                default: return false;
            }
        }

        /// <summary>
        ///     找出運算式裡第一個欄位，讓另一邊的值可以依這個欄位決定參數名稱與型別，例如 <c>(x.Id &amp; 1) &gt; 2</c> 的 <c>2</c> 對應到 <c>Id</c>。
        /// </summary>
        private static PropertyMetadata FindProperty(SqlExpression expression)
        {
            switch (expression)
            {
                case ColumnExpression column: return column.Property;
                case SqlBinaryExpression binary: return FindProperty(binary.Left) ?? FindProperty(binary.Right);
                case SqlUnaryExpression unary: return FindProperty(unary.Operand);
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

        /// <summary>
        ///     <see cref="PartialEvaluator" /> 產生的參數名稱是 <c>__age_0</c>，取出中間的 <c>age</c>。
        /// </summary>
        private static string GetBaseName(string parameterName)
        {
            var name = parameterName.StartsWith("__", StringComparison.Ordinal) ? parameterName.Substring(2) : parameterName;
            var underscore = name.LastIndexOf('_');

            return underscore > 0 && name.Skip(underscore + 1).All(char.IsDigit) ? name.Substring(0, underscore) : name;
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

            return this.ToPredicate(this.Translate(expression, null), true, expression);
        }

        /// <summary>
        ///     把值轉成條件：布林欄位變成 <c>[IsActive] = {=IsActive_0}</c>、布林參數變成 <c>{=flag_0} = 1</c>；
        ///     <paramref name="value" /> 為 <c>false</c> 時是否定的條件。
        /// </summary>
        private SqlExpression ToPredicate(SqlExpression sql, bool value, Expression source)
        {
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
                case MemberExpression member: return this.TranslateMember(member);
                case ConstantExpression constant: return this.context.CreateParameter(null, constant.Value, constant.Type, hint);
                case ParameterExpression parameter: return this.TranslateParameter(parameter, hint);
                default: throw this.NotTranslatable(expression);
            }
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

                case ExpressionType.Equal when this.IsNullValue(binary.Right):
                    return new SqlUnaryExpression(SqlUnaryOperator.IsNull, this.Translate(binary.Left, hint), typeof(bool));

                case ExpressionType.Equal when this.IsNullValue(binary.Left):
                    return new SqlUnaryExpression(SqlUnaryOperator.IsNull, this.Translate(binary.Right, hint), typeof(bool));

                case ExpressionType.NotEqual when this.IsNullValue(binary.Right):
                    return new SqlUnaryExpression(SqlUnaryOperator.IsNotNull, this.Translate(binary.Left, hint), typeof(bool));

                case ExpressionType.NotEqual when this.IsNullValue(binary.Left):
                    return new SqlUnaryExpression(SqlUnaryOperator.IsNotNull, this.Translate(binary.Right, hint), typeof(bool));
            }

            var (@operator, isComparison) = this.GetOperator(binary, isBoolean);

            // 先翻譯含欄位的一邊，值的那一邊才能依欄位決定參數名稱與型別，例如 30 < x.Age。
            SqlExpression left, right;

            if (this.IsValue(binary.Left) && !this.IsValue(binary.Right))
            {
                right = this.Translate(binary.Right, hint);
                left = this.Translate(binary.Left, FindProperty(right) ?? hint);
            }
            else
            {
                left = this.Translate(binary.Left, hint);
                right = this.Translate(binary.Right, FindProperty(left) ?? hint);
            }

            return new SqlBinaryExpression(@operator, left, right, isComparison ? typeof(bool) : binary.Type);
        }

        private (SqlBinaryOperator Operator, bool IsComparison) GetOperator(BinaryExpression binary, bool isBoolean)
        {
            switch (binary.NodeType)
            {
                case ExpressionType.Equal: return (SqlBinaryOperator.Equal, true);
                case ExpressionType.NotEqual: return (SqlBinaryOperator.NotEqual, true);
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

        private SqlExpression TranslateMember(MemberExpression member)
        {
            if (member.Expression is ParameterExpression parameter && this.tables.TryGetValue(parameter, out var table))
            {
                var property = table.Entity.FindColumn(member.Member.Name);

                if (property == null) throw new NotSupportedException($"'{member}' is not mapped to a column. ({this.lambda})");

                return table.Column(property);
            }

            throw this.NotTranslatable(member);
        }

        private SqlExpression TranslateParameter(ParameterExpression parameter, PropertyMetadata hint)
        {
            if (this.context.TryGetParameterValue(parameter, out var value))
            {
                return this.context.CreateParameter(GetBaseName(parameter.Name), value, parameter.Type, hint);
            }

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
