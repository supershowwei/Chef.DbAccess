using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 攔截 3.x 要送往資料庫的 SQL 與參數，不建立任何連線。
    /// 只覆寫目前的快照測試會走到的 Execute 方法，其他方法仍然會真的連線。
    /// </summary>
    internal class CapturingDataAccess<T> : SqlServerDataAccess<T>
    {
        // 不會真的連線，只是讓 3.x 有一個合法的連線字串可以解析
        private const string UnusedConnectionString = @"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Club;Integrated Security=True";

        private readonly List<string> commands = new List<string>();

        public CapturingDataAccess()
            : base(null, UnusedConnectionString)
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

                case IEnumerable items:
                    return $"[{string.Join(", ", items.Cast<object>().Select(FormatValue))}] ({value.GetType().Name})";

                default:
                    return $"{Convert.ToString(value, CultureInfo.InvariantCulture)} ({value.GetType().Name})";
            }
        }
    }
}
