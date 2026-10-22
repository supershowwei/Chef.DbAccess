using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把 <see cref="TranslatedQuery.Shaper" /> 編譯成從 <see cref="DbDataReader" /> 組出一筆結果的 delegate：
    ///     <list type="bullet">
    ///         <item><see cref="ProjectionBindingExpression" /> 換成讀取對應序號（ordinal）的欄位值。</item>
    ///         <item>留在用戶端的查詢參數（例如 <c>Tag = tag</c> 的 <c>__tag_0</c>）換成從這次執行的參數值取值，所以同一個 delegate 可以快取重複使用。</item>
    ///     </list>
    ///     <para>
    ///         欄位值是 <c>NULL</c> 時給 <c>default</c>（不可為 null 的數值型別就是 0），與 Dapper 填入屬性時的行為相同。
    ///         資料庫的型別與屬性不完全相同時會轉換，例如 <c>CASE WHEN ... THEN 1</c> 的 <c>int</c> 轉成 <see cref="bool" />、
    ///         <c>tinyint</c> 轉成 <see cref="int" />、數字轉成列舉。
    ///     </para>
    /// </summary>
    internal static class ShaperCompiler
    {
        private static readonly MethodInfo ReadValueMethod = typeof(ShaperCompiler).GetMethod(nameof(ReadValue), BindingFlags.Public | BindingFlags.Static);

        private static readonly PropertyInfo ParameterValueIndexer = typeof(IReadOnlyDictionary<string, object>).GetProperty("Item");

        public static Func<DbDataReader, IReadOnlyDictionary<string, object>, T> Compile<T>(Expression shaper)
        {
            if (shaper == null) throw new ArgumentNullException(nameof(shaper));

            var reader = Expression.Parameter(typeof(DbDataReader), "reader");
            var parameterValues = Expression.Parameter(typeof(IReadOnlyDictionary<string, object>), "parameterValues");

            var body = new ShaperRewriter(reader, parameterValues).Visit(shaper);

            if (body.Type != typeof(T)) body = Expression.Convert(body, typeof(T));

            return Expression.Lambda<Func<DbDataReader, IReadOnlyDictionary<string, object>, T>>(body, reader, parameterValues).Compile();
        }

        /// <summary>
        ///     讀取欄位值並轉成 <typeparamref name="T" />；值是 <c>NULL</c> 時回傳 <c>default</c>。由編譯後的 Shaper 呼叫。
        /// </summary>
        public static T ReadValue<T>(DbDataReader reader, int ordinal)
        {
            var value = reader.GetValue(ordinal);

            return value is DBNull ? default : ConvertValue<T>(value);
        }

        private static T ConvertValue<T>(object value)
        {
            if (value is T typed) return typed;

            var type = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

            if (type.IsEnum) return (T)Enum.ToObject(type, value);

            try
            {
                return (T)Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is InvalidCastException || ex is FormatException || ex is OverflowException)
            {
                throw new InvalidCastException($"Can not convert the column value '{value}' ({value.GetType()}) to '{typeof(T)}'.", ex);
            }
        }

        private sealed class ShaperRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression reader;
            private readonly ParameterExpression parameterValues;
            private readonly HashSet<ParameterExpression> lambdaParameters = new HashSet<ParameterExpression>();

            public ShaperRewriter(ParameterExpression reader, ParameterExpression parameterValues)
            {
                this.reader = reader;
                this.parameterValues = parameterValues;
            }

            protected override Expression VisitExtension(Expression node)
            {
                if (node is ProjectionBindingExpression projection)
                {
                    return Expression.Call(ReadValueMethod.MakeGenericMethod(projection.Type), this.reader, Expression.Constant(projection.Index));
                }

                return base.VisitExtension(node);
            }

            protected override Expression VisitLambda<TDelegate>(Expression<TDelegate> node)
            {
                this.lambdaParameters.UnionWith(node.Parameters);

                return base.VisitLambda(node);
            }

            /// <summary>
            ///     不是 lambda 宣告的參數就是 <see cref="PartialEvaluator" /> 產生的查詢參數，改成 <c>(T)parameterValues["__tag_0"]</c>。
            /// </summary>
            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (this.lambdaParameters.Contains(node)) return node;

                return Expression.Convert(Expression.Property(this.parameterValues, ParameterValueIndexer, Expression.Constant(node.Name)), node.Type);
            }
        }
    }
}
