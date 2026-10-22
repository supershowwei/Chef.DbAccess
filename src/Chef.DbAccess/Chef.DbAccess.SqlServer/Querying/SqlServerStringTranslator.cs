using System;
using System.Collections.Generic;
using System.Reflection;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     SQL Server 的字串函式：
    ///     <list type="bullet">
    ///         <item><c>x.Name.Length</c> → <c>LEN([Name])</c>。注意 <c>LEN</c> 不計結尾空白，<c>"a "</c> 在 C# 是 2、在 SQL Server 是 1。</item>
    ///         <item><c>ToUpper()</c> → <c>UPPER</c>、<c>ToLower()</c> → <c>LOWER</c>。</item>
    ///         <item><c>Trim()</c> → <c>LTRIM(RTRIM(...))</c>，SQL Server 2017 之前沒有 <c>TRIM</c>。</item>
    ///         <item><c>Substring(start[, length])</c> → <c>SUBSTRING(..., start + 1, length)</c>，SQL Server 從 1 起算；沒有長度時取到結尾。</item>
    ///     </list>
    ///     <para>只處理不帶 <c>CultureInfo</c>、<c>char[]</c> 等參數的多載，其他多載交回翻譯器丟出 <see cref="NotSupportedException" />。</para>
    /// </summary>
    internal sealed class SqlServerStringTranslator : IMemberTranslator, IMethodCallTranslator
    {
        public static readonly SqlServerStringTranslator Instance = new SqlServerStringTranslator();

        private SqlServerStringTranslator()
        {
        }

        public SqlExpression Translate(SqlExpression instance, MemberInfo member, Type returnType)
        {
            if (member.DeclaringType == typeof(string) && member.Name == nameof(string.Length)) return Len(instance);

            return null;
        }

        public SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (method.DeclaringType != typeof(string) || instance == null) return null;

            switch (method.Name)
            {
                case nameof(string.ToUpper) when arguments.Count == 0: return Function("UPPER", instance);
                case nameof(string.ToLower) when arguments.Count == 0: return Function("LOWER", instance);
                case nameof(string.Trim) when arguments.Count == 0: return Function("LTRIM", Function("RTRIM", instance));

                case nameof(string.Substring) when arguments.Count == 1:
                    return new SqlFunctionExpression("SUBSTRING", new[] { instance, OneBased(arguments[0]), Len(instance) }, typeof(string));

                case nameof(string.Substring) when arguments.Count == 2:
                    return new SqlFunctionExpression("SUBSTRING", new[] { instance, OneBased(arguments[0]), arguments[1] }, typeof(string));

                default: return null;
            }
        }

        private static SqlExpression Len(SqlExpression instance)
        {
            return new SqlFunctionExpression("LEN", new[] { instance }, typeof(int));
        }

        private static SqlExpression Function(string name, SqlExpression argument)
        {
            return new SqlFunctionExpression(name, new[] { argument }, typeof(string));
        }

        private static SqlExpression OneBased(SqlExpression index)
        {
            return new SqlBinaryExpression(SqlBinaryOperator.Add, index, new SqlConstantExpression(1, typeof(int)), typeof(int));
        }
    }
}
