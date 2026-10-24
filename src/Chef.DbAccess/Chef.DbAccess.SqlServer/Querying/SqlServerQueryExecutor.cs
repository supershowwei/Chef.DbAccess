using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlServer.SqlGeneration;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     在 SQL Server 執行 <see cref="PreparedQuery" />。
    ///     <list type="bullet">
    ///         <item>參數由 <see cref="SqlServerParameterMapper" /> 依欄位型別轉換（<c>varchar</c> 送成 DbString 等）。</item>
    ///         <item>命令交給 Dapper 建立，數值、布林與列舉參數的 <c>{=name}</c> 由 Dapper 寫入 SQL（與 3.x 相同）。</item>
    ///         <item>讀取資料列時使用編譯好的 Shaper，不經過 Dapper 的物件對應。</item>
    ///     </list>
    ///     <para>每次查詢開一條連線，列舉結束（或中途停止）時關閉；資料列一邊讀一邊回傳，不會先全部載入記憶體。</para>
    /// </summary>
    internal sealed class SqlServerQueryExecutor : IQueryExecutor
    {
        private readonly string connectionString;

        public SqlServerQueryExecutor(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("Connection string can not be null or empty.", nameof(connectionString));

            this.connectionString = connectionString;
        }

        public IEnumerable<T> Query<T>(PreparedQuery query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            var shaper = query.Compiled.GetShaper<T>();

            using (var connection = new SqlConnection(this.connectionString))
            using (var reader = ExecuteReader(connection, query))
            {
                while (reader.Read()) yield return shaper(reader, query.ParameterValues);
            }
        }

        public bool HasRows(PreparedQuery query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            using (var connection = new SqlConnection(this.connectionString))
            using (var reader = ExecuteReader(connection, query))
            {
                return reader.Read();
            }
        }

        /// <summary>
        ///     Dapper 會自動開啟關閉中的連線；讀取器一律是 <see cref="DbDataReader" />（SqlClient 的讀取器經 Dapper 包裝後仍是）。
        /// </summary>
        private static DbDataReader ExecuteReader(SqlConnection connection, PreparedQuery query)
        {
            var command = new CommandDefinition(query.Sql, SqlServerParameterMapper.ToDynamicParameters(query.Parameters));
            var reader = connection.ExecuteReader(command);

            return reader as DbDataReader ?? throw new InvalidOperationException($"Expected a DbDataReader but got '{reader.GetType()}'.");
        }
    }
}
