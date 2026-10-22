using System;
using System.Collections.Generic;
using System.Reflection;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     SQL Server 的 <see cref="DateTime" /> 與 <see cref="DateTimeOffset" /> 成員與方法：
    ///     <list type="bullet">
    ///         <item><c>Year</c>、<c>Month</c>、<c>Day</c>、<c>Hour</c>、<c>Minute</c>、<c>Second</c>、<c>Millisecond</c> → <c>DATEPART(year, ...)</c>……</item>
    ///         <item><c>DateTime.Date</c> → <c>CONVERT(date, ...)</c>。</item>
    ///         <item>
    ///             <c>AddYears</c>、<c>AddMonths</c>、<c>AddDays</c>、<c>AddHours</c>、<c>AddMinutes</c>、<c>AddSeconds</c>、<c>AddMilliseconds</c> → <c>DATEADD(day, n, ...)</c>……
    ///             <c>DATEADD</c> 會捨去小數，所以接受小數的方法把小數部分換算成毫秒另外加上，<c>AddDays(1.5)</c> 一樣是一天半；
    ///             精確度到毫秒（四捨五入），與 .NET Framework 相同。
    ///         </item>
    ///     </list>
    ///     <para><c>DateTime.Now</c>、<c>DateTime.Today</c> 等不依賴欄位的值已經由 PartialEvaluator 在用戶端求值，不會翻譯成 <c>GETDATE()</c>。</para>
    /// </summary>
    internal sealed class SqlServerDateTimeTranslator : IMemberTranslator, IMethodCallTranslator
    {
        public static readonly SqlServerDateTimeTranslator Instance = new SqlServerDateTimeTranslator();

        private static readonly Dictionary<string, string> DateParts = new Dictionary<string, string>
                                                                       {
                                                                           [nameof(DateTime.Year)] = "year",
                                                                           [nameof(DateTime.Month)] = "month",
                                                                           [nameof(DateTime.Day)] = "day",
                                                                           [nameof(DateTime.Hour)] = "hour",
                                                                           [nameof(DateTime.Minute)] = "minute",
                                                                           [nameof(DateTime.Second)] = "second",
                                                                           [nameof(DateTime.Millisecond)] = "millisecond"
                                                                       };

        // 接受小數的方法記錄一個單位是多少毫秒，小數部分換算成毫秒另外加上。
        private static readonly Dictionary<string, (string DatePart, int? Milliseconds)> AddMethods =
            new Dictionary<string, (string DatePart, int? Milliseconds)>
            {
                [nameof(DateTime.AddYears)] = ("year", null),
                [nameof(DateTime.AddMonths)] = ("month", null),
                [nameof(DateTime.AddDays)] = ("day", 86400000),
                [nameof(DateTime.AddHours)] = ("hour", 3600000),
                [nameof(DateTime.AddMinutes)] = ("minute", 60000),
                [nameof(DateTime.AddSeconds)] = ("second", 1000),
                [nameof(DateTime.AddMilliseconds)] = ("millisecond", 1)
            };

        private SqlServerDateTimeTranslator()
        {
        }

        public SqlExpression Translate(SqlExpression instance, MemberInfo member, Type returnType)
        {
            if (!IsDateTime(member.DeclaringType)) return null;

            if (DateParts.TryGetValue(member.Name, out var datePart))
            {
                return new SqlFunctionExpression("DATEPART", new[] { new SqlFragmentExpression(datePart), instance }, typeof(int));
            }

            if (member.DeclaringType == typeof(DateTime) && member.Name == nameof(DateTime.Date))
            {
                return new SqlFunctionExpression("CONVERT", new[] { new SqlFragmentExpression("date"), instance }, typeof(DateTime));
            }

            return null;
        }

        public SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (instance == null || !IsDateTime(method.DeclaringType) || arguments.Count != 1) return null;

            if (!AddMethods.TryGetValue(method.Name, out var add)) return null;

            var value = arguments[0];

            // AddYears、AddMonths 只接受整數。
            if (add.Milliseconds == null) return DateAdd(add.DatePart, value, instance, method.ReturnType);

            // AddMilliseconds(1.5)：DATEADD 會捨去小數，先四捨五入到毫秒。
            if (add.Milliseconds == 1) return DateAdd(add.DatePart, Round(value), instance, method.ReturnType);

            // AddDays(1.5)：整數部分用原本的單位，小數部分換算成毫秒，例如
            // DATEADD(millisecond, ROUND((1.5 - FLOOR(1.5)) * 86400000, 0), DATEADD(day, FLOOR(1.5), x))。
            // 用 FLOOR 而不是捨去，負數也正確：-1.5 天 = -2 天 + 12 小時。
            var whole = new SqlFunctionExpression("FLOOR", new[] { value }, typeof(double));
            var fraction = new SqlBinaryExpression(SqlBinaryOperator.Subtract, value, whole, typeof(double));
            var milliseconds = new SqlBinaryExpression(SqlBinaryOperator.Multiply, fraction, new SqlConstantExpression(add.Milliseconds.Value, typeof(int)), typeof(double));

            return DateAdd("millisecond", Round(milliseconds), DateAdd(add.DatePart, whole, instance, method.ReturnType), method.ReturnType);
        }

        private static SqlExpression DateAdd(string datePart, SqlExpression number, SqlExpression date, Type type)
        {
            return new SqlFunctionExpression("DATEADD", new[] { new SqlFragmentExpression(datePart), number, date }, type);
        }

        private static SqlExpression Round(SqlExpression value)
        {
            return new SqlFunctionExpression("ROUND", new[] { value, new SqlConstantExpression(0, typeof(int)) }, typeof(double));
        }

        private static bool IsDateTime(Type type)
        {
            return type == typeof(DateTime) || type == typeof(DateTimeOffset);
        }
    }
}
