using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 攔截 3.x 要送往資料庫的 SQL 與參數，不建立任何連線。
    /// 只覆寫目前的快照測試會走到的 Execute 方法；走到沒覆寫的方法會在建立連線時丟出例外。
    /// </summary>
    internal class CapturingDataAccess<T> : SqlServerDataAccess<T>
    {
        // SqlConnection 不認得這個關鍵字，沒覆寫到的 Execute 方法一建立連線就會失敗，
        // 不會在 LocalDB 開著的時候默默查詢真正的資料庫
        private const string NeverConnect = ";Capture Only=True";

        private readonly List<string> commands = new List<string>();

        public CapturingDataAccess()
            : base(null, ResolveConnectionString() + NeverConnect)
        {
        }

        /// <summary>
        /// 依執行順序列出攔截到的命令，每個命令是 SQL 加上參數清單。
        /// </summary>
        public string CapturedCommands => string.Join("\n\n", this.commands);

        protected override Task<List<TResult>> ExecuteQueryAsync<TResult>(string sql, object param)
        {
            this.Capture(sql, param);

            return Task.FromResult(new List<TResult>());
        }

        // 和 SqlServerDataAccessFactory.Create<T>() 一樣從模型的 [ConnectionString] 取得連線字串：
        // JOIN 時 3.x 會比對兩邊的資料庫名稱，決定要不要加上 [資料庫].[結構描述] 前綴
        private static string ResolveConnectionString()
        {
            var attribute = typeof(T).GetCustomAttributes<ConnectionStringAttribute>(true).Single();

            return SqlServerDataAccessFactory.Instance.GetConnectionString(attribute.ConnectionString);
        }

        private void Capture(string sql, object param)
        {
            var builder = new StringBuilder(sql.Replace("\r\n", "\n").Trim());

            foreach (var parameter in GetParameters(param))
            {
                builder.Append($"\n-- @{parameter.Key} = {FormatValue(parameter.Value)}");
            }

            this.commands.Add(builder.ToString());
        }

        private static IEnumerable<KeyValuePair<string, object>> GetParameters(object param)
        {
            switch (param)
            {
                case null:
                    return Enumerable.Empty<KeyValuePair<string, object>>();

                case IDictionary<string, object> dictionary:
                    return dictionary;

                default:
                    throw new NotSupportedException($"尚未支援的參數型別：{param.GetType()}");
            }
        }

        // 一併記錄 CLR 型別：3.x 傳給 Dapper 的型別決定了 SqlParameter 的 SqlDbType
        private static string FormatValue(object value)
        {
            switch (value)
            {
                case null:
                    return "NULL";

                case string text:
                    return $"'{text}' (String)";

                // 保留毫秒與 DateTimeKind，預設格式兩者都會遺失
                case DateTime dateTime:
                    return $"{dateTime:O} (DateTime)";

                case IEnumerable items:
                    return $"[{string.Join(", ", items.Cast<object>().Select(FormatValue))}] ({value.GetType().Name})";

                default:
                    return $"{Convert.ToString(value, CultureInfo.InvariantCulture)} ({value.GetType().Name})";
            }
        }
    }
}
