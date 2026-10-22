using System;
using System.Collections.Generic;
using System.Reflection;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     SQL Server 的 <see cref="Math" /> 函式：
    ///     <list type="bullet">
    ///         <item><c>Math.Abs</c> → <c>ABS</c>、<c>Math.Floor</c> → <c>FLOOR</c>、<c>Math.Ceiling</c> → <c>CEILING</c>。</item>
    ///         <item>
    ///             <c>Math.Round(x)</c> → <c>ROUND(x, 0)</c>、<c>Math.Round(x, digits)</c> → <c>ROUND(x, digits)</c>。
    ///             注意 C# 預設是銀行家捨入（<c>Math.Round(2.5)</c> 為 2），SQL Server 是四捨五入（3）。
    ///             帶 <see cref="MidpointRounding" /> 的多載不翻譯。
    ///         </item>
    ///     </list>
    /// </summary>
    internal sealed class SqlServerMathTranslator : IMethodCallTranslator
    {
        public static readonly SqlServerMathTranslator Instance = new SqlServerMathTranslator();

        private SqlServerMathTranslator()
        {
        }

        public SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (method.DeclaringType != typeof(Math)) return null;

            switch (method.Name)
            {
                case nameof(Math.Abs) when arguments.Count == 1: return Function("ABS", method, arguments[0]);
                case nameof(Math.Floor) when arguments.Count == 1: return Function("FLOOR", method, arguments[0]);
                case nameof(Math.Ceiling) when arguments.Count == 1: return Function("CEILING", method, arguments[0]);

                case nameof(Math.Round) when arguments.Count == 1:
                    return Function("ROUND", method, arguments[0], new SqlConstantExpression(0, typeof(int)));

                case nameof(Math.Round) when arguments.Count == 2 && method.GetParameters()[1].ParameterType == typeof(int):
                    return Function("ROUND", method, arguments[0], arguments[1]);

                default: return null;
            }
        }

        private static SqlExpression Function(string name, MethodInfo method, params SqlExpression[] arguments)
        {
            return new SqlFunctionExpression(name, arguments, method.ReturnType);
        }
    }
}
