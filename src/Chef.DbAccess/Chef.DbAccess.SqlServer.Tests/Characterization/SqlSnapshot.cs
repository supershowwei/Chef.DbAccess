using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    ///     攔截 SqlServerDataAccess 產生的 SQL（不連線資料庫），並與 Snapshots 目錄下的快照比對。
    ///     <para>設定環境變數 CHEF_UPDATE_SNAPSHOTS=1 可覆寫既有快照；快照不存在時會自動建立（CI 環境下則視為失敗）。</para>
    ///     <para>參數取自 OutputSql 回呼；3.x 的 Update / Delete / Upsert / Bulk 與 Insert(entity) 回呼時不帶參數，所以這些快照只有 SQL。</para>
    /// </summary>
    internal static class SqlSnapshot
    {
        // 萬一有路徑在產生 SQL 之前就嘗試連線，讓它快速失敗，而不是卡在逾時。
        private const string UnreachableConnectionString = "Data Source=127.0.0.1,1;Initial Catalog=Snapshot;Connect Timeout=1;";

        private static readonly Regex GuidRegex = new Regex("[0-9a-fA-F]{32}", RegexOptions.Compiled);

        public static SqlServerDataAccess<T> CreateDataAccess<T>(string tableName = null)
        {
            return new SqlServerDataAccess<T>(tableName, UnreachableConnectionString)
                   {
                       OutputSql = (sql, param) => throw new SqlCapturedException(sql, param)
                   };
        }

        public static async Task MatchAsync(Func<Task> action, [CallerMemberName] string name = null, [CallerFilePath] string callerFile = null)
        {
            string actual;

            try
            {
                await action();

                throw new AssertFailedException("沒有攔截到任何 SQL。");
            }
            catch (SqlCapturedException ex)
            {
                actual = Format(ex.Sql, ex.Param);
            }
            catch (DbException)
            {
                throw;
            }
            catch (Exception ex) when (!(ex is UnitTestAssertException))
            {
                actual = $"-- Exception: {ex.GetType().FullName}: {ex.Message}";
            }

            Verify(actual, name, callerFile);
        }

        private static void Verify(string actual, string name, string callerFile)
        {
            var directory = Path.Combine(Path.GetDirectoryName(callerFile), "Snapshots", Path.GetFileNameWithoutExtension(callerFile));
            var verifiedPath = Path.Combine(directory, name + ".sql");
            var receivedPath = Path.Combine(directory, name + ".received.sql");

            Directory.CreateDirectory(directory);

            if (Environment.GetEnvironmentVariable("CHEF_UPDATE_SNAPSHOTS") == "1")
            {
                WriteSnapshot(verifiedPath, actual);

                return;
            }

            if (!File.Exists(verifiedPath))
            {
                if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CI")))
                {
                    WriteSnapshot(receivedPath, actual);

                    Assert.Fail($"缺少快照 {verifiedPath}，請在本機執行測試建立後一併提交。");
                }

                WriteSnapshot(verifiedPath, actual);

                Assert.Inconclusive($"已建立新快照 {verifiedPath}，請檢查內容後再執行一次。");
            }

            var expected = Normalize(File.ReadAllText(verifiedPath));

            if (expected == actual)
            {
                if (File.Exists(receivedPath)) File.Delete(receivedPath);

                return;
            }

            WriteSnapshot(receivedPath, actual);

            Assert.Fail($"SQL 與快照不符，實際輸出已寫入 {receivedPath}{Environment.NewLine}--- 預期 ---{Environment.NewLine}{expected}{Environment.NewLine}--- 實際 ---{Environment.NewLine}{actual}");
        }

        private static void WriteSnapshot(string path, string content)
        {
            File.WriteAllText(path, content + "\n");
        }

        private static string Format(string sql, object param)
        {
            var sb = new StringBuilder();

            sb.Append(Normalize(GuidRegex.Replace(sql, "{guid}")));

            var parameters = EnumerateParameters(param).ToList();

            if (parameters.Count > 0)
            {
                sb.Append("\n\n-- Parameters");

                foreach (var (key, value) in parameters)
                {
                    sb.Append($"\n-- @{key} = {FormatValue(value)}");
                }
            }

            return sb.ToString();
        }

        private static IEnumerable<(string, object)> EnumerateParameters(object param)
        {
            switch (param)
            {
                case null: yield break;

                case IDictionary<string, object> dictionary:
                    foreach (var pair in dictionary) yield return (pair.Key, pair.Value);
                    yield break;

                default:
                    foreach (var property in param.GetType().GetProperties()) yield return (property.Name, property.GetValue(param));
                    yield break;
            }
        }

        private static string FormatValue(object value)
        {
            switch (value)
            {
                case null: return "NULL";
                case DbString dbString: return $"DbString({FormatValue(dbString.Value)}, Length={dbString.Length}, IsAnsi={dbString.IsAnsi}, IsFixedLength={dbString.IsFixedLength})";
                case string str: return $"'{str}' (String)";
                case DateTime dateTime: return $"{dateTime.ToString("O", CultureInfo.InvariantCulture)} (DateTime)";
                case DateTimeOffset dateTimeOffset: return $"{dateTimeOffset.ToString("O", CultureInfo.InvariantCulture)} (DateTimeOffset)";
                case IEnumerable enumerable: return $"[{string.Join(", ", enumerable.Cast<object>().Select(FormatValue))}]";
                case IFormattable formattable: return $"{formattable.ToString(null, CultureInfo.InvariantCulture)} ({value.GetType().Name})";
                default: return $"{value} ({value.GetType().Name})";
            }
        }

        private static string Normalize(string text)
        {
            return text.Replace("\r\n", "\n").Trim();
        }

        private class SqlCapturedException : Exception
        {
            public SqlCapturedException(string sql, object param)
            {
                this.Sql = sql;
                this.Param = param;
            }

            public string Sql { get; }

            public object Param { get; }
        }
    }
}
