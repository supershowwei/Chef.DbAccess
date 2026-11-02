using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
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

        // 標出 3.x 自行開啟的交易範圍（BeginTransaction／Commit）
        private const string BeginTransaction = "-- BEGIN TRANSACTION";
        private const string CommitTransaction = "-- COMMIT TRANSACTION";

        private static readonly Regex TempTableNameRegex = new Regex("#_[0-9a-f]{32}");

        private readonly List<string> commands = new List<string>();

        public CapturingDataAccess()
            : this(null)
        {
        }

        /// <summary>
        /// 對應 SqlServerDataAccessFactory.Create&lt;T&gt;(null, nameOrConnectionString)。
        /// </summary>
        public CapturingDataAccess(string nameOrConnectionString)
            : base(null, ResolveConnectionString(nameOrConnectionString) + NeverConnect)
        {
        }

        /// <summary>
        /// 依執行順序列出攔截到的命令，每個命令是 SQL 加上參數清單（JOIN 另外附上 splitOn）。
        /// </summary>
        public string CapturedCommands => ScrubTempTableNames(string.Join("\n\n", this.commands));

        protected override Task<List<TResult>> ExecuteQueryAsync<TResult>(string sql, object param)
        {
            this.Capture(sql, param);

            return Task.FromResult(new List<TResult>());
        }

        protected override Task<TResult> ExecuteQueryOneAsync<TResult>(string sql, object param)
        {
            this.Capture(sql, param);

            return Task.FromResult(default(TResult));
        }

        protected override Task<int> ExecuteCommandAsync(string sql, object param)
        {
            this.Capture(sql, param);

            return Task.FromResult(0);
        }

        // 交易版本：照 3.x 的執行順序記錄，postSql／retractionSql 是在 Commit 之後、交易之外執行

        protected override Task<int> ExecuteTransactionalCommandAsync(
            string sql,
            object param,
            string declarationSql = null,
            string retractionSql = null)
        {
            this.CaptureIfAny(declarationSql, null);
            this.commands.Add(BeginTransaction);
            this.Capture(sql, param);
            this.commands.Add(CommitTransaction);
            this.CaptureIfAny(retractionSql, null);

            return Task.FromResult(0);
        }

        protected override Task<List<TResult>> ExecuteTransactionalQueryAsync<TResult>(
            string sql,
            object param,
            string resultSql,
            object resultParam,
            string preSql = null,
            object preParam = null,
            string postSql = null,
            object postParam = null)
        {
            this.commands.Add(BeginTransaction);
            this.CaptureIfAny(preSql, preParam);
            this.Capture(sql, param);
            this.Capture(resultSql, resultParam);
            this.commands.Add(CommitTransaction);
            this.CaptureIfAny(postSql, postParam);

            return Task.FromResult(new List<TResult>());
        }

        // 以下是 JOIN 用的多表版本，setter 只負責組合物件，不影響送出的 SQL

        protected override Task<List<TResult>> ExecuteQueryAsync<TSecond, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(new List<TResult>());
        }

        protected override Task<List<TResult>> ExecuteQueryAsync<TSecond, TThird, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(new List<TResult>());
        }

        protected override Task<List<TResult>> ExecuteQueryAsync<TSecond, TThird, TFourth, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(new List<TResult>());
        }

        protected override Task<List<TResult>> ExecuteQueryAsync<TSecond, TThird, TFourth, TFifth, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth> fifthSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(new List<TResult>());
        }

        protected override Task<List<TResult>> ExecuteQueryAsync<TSecond, TThird, TFourth, TFifth, TSixth, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth> fifthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth, TSixth> sixthSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(new List<TResult>());
        }

        protected override Task<List<TResult>> ExecuteQueryAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth> fifthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth, TSixth> sixthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh> seventhSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(new List<TResult>());
        }

        protected override Task<TResult> ExecuteQueryOneAsync<TSecond, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(default(TResult));
        }

        protected override Task<TResult> ExecuteQueryOneAsync<TSecond, TThird, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(default(TResult));
        }

        protected override Task<TResult> ExecuteQueryOneAsync<TSecond, TThird, TFourth, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(default(TResult));
        }

        protected override Task<TResult> ExecuteQueryOneAsync<TSecond, TThird, TFourth, TFifth, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth> fifthSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(default(TResult));
        }

        protected override Task<TResult> ExecuteQueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth> fifthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth, TSixth> sixthSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(default(TResult));
        }

        protected override Task<TResult> ExecuteQueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TResult>(
            string sql,
            Action<TResult, TSecond> secondSetter,
            Action<TResult, TSecond, TThird> thirdSetter,
            Action<TResult, TSecond, TThird, TFourth> fourthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth> fifthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth, TSixth> sixthSetter,
            Action<TResult, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh> seventhSetter,
            object parameters,
            string splitOn)
        {
            this.Capture(sql, parameters, splitOn);

            return Task.FromResult(default(TResult));
        }

        // 和 SqlServerDataAccessFactory.Create<T>() 一樣從模型的 [ConnectionString] 取得連線字串：
        // JOIN 時 3.x 會比對兩邊的資料庫名稱，決定要不要加上 [資料庫].[結構描述] 前綴
        private static string ResolveConnectionString(string nameOrConnectionString)
        {
            var attributes = typeof(T).GetCustomAttributes<ConnectionStringAttribute>(true);

            var attribute = string.IsNullOrEmpty(nameOrConnectionString)
                                ? attributes.Single()
                                : attributes.SingleOrDefault(x => x.ConnectionString == nameOrConnectionString);

            return attribute != null
                       ? SqlServerDataAccessFactory.Instance.GetConnectionString(attribute.ConnectionString)
                       : nameOrConnectionString;
        }

        private void Capture(string sql, object param, string splitOn = null)
        {
            var builder = new StringBuilder(sql.Replace("\r\n", "\n").Trim());

            foreach (var line in FormatParameters(sql, param))
            {
                builder.Append($"\n-- {line}");
            }

            // Dapper 依 splitOn 的欄位名稱把一列切成多個物件
            if (splitOn != null)
            {
                builder.Append($"\n-- splitOn: {splitOn}");
            }

            this.commands.Add(builder.ToString());
        }

        // 3.x 的暫存資料表名稱是 #_ 加上 Guid，每次執行都不同；依出現順序改成 #_TempTable1、#_TempTable2…
        private static string ScrubTempTableNames(string text)
        {
            var names = new Dictionary<string, string>();

            return TempTableNameRegex.Replace(
                text,
                match =>
                    {
                        if (!names.TryGetValue(match.Value, out var name))
                        {
                            names.Add(match.Value, name = $"#_TempTable{names.Count + 1}");
                        }

                        return name;
                    });
        }

        private void CaptureIfAny(string sql, object param)
        {
            if (!string.IsNullOrEmpty(sql)) this.Capture(sql, param);
        }

        private static IEnumerable<string> FormatParameters(string sql, object param)
        {
            switch (param)
            {
                case null:
                    return Enumerable.Empty<string>();

                case IDictionary<string, object> dictionary:
                    return dictionary.Select(x => $"@{x.Key} = {FormatValue(x.Value)}");

                // 多筆寫入：Dapper 對每個元素各執行一次同一段 SQL
                case IEnumerable rows:
                    return rows.Cast<object>().SelectMany((row, index) => FormatParameters(sql, row).Select(line => $"[{index}] {line}"));

                // 實體或匿名物件：Dapper 只送出 SQL 裡有用到的屬性（@Name 或字面值替換的 {=Name}）
                default:
                    return param.GetType()
                        .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                        .Where(x => Regex.IsMatch(sql, $@"(@|\{{=){x.Name}(?![\p{{L}}\p{{N}}_])", RegexOptions.IgnoreCase))
                        .Select(x => $"@{x.Name} = {FormatValue(x.GetValue(param))}");
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
