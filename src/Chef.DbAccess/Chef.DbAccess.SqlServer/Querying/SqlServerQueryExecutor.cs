using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
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
    ///         <item>
    ///             與 3.x 相同，執行前呼叫 <c>OutputSql(sql, parameters)</c>，資料庫丟出例外時呼叫 <c>OnDbError(exception, sql, parameters)</c> 後再丟出；
    ///             取消（<see cref="OperationCanceledException" />）不是資料庫錯誤，不呼叫 <c>OnDbError</c>。
    ///         </item>
    ///     </list>
    ///     <para>
    ///         每次查詢開一條連線。同步的 <see cref="Query{T}" /> 一邊讀一邊回傳，列舉結束（或中途停止）時關閉連線；
    ///         非同步的 <see cref="QueryAsync{T}" /> 讀完所有資料列後回傳，<see cref="CancellationToken" /> 會傳給開啟連線、執行命令與讀取資料列。
    ///     </para>
    /// </summary>
    internal sealed class SqlServerQueryExecutor : IQueryExecutor
    {
        private readonly string connectionString;
        private readonly Action<string, object> outputSql;
        private readonly Action<Exception, string, object> onDbError;

        public SqlServerQueryExecutor(string connectionString, Action<string, object> outputSql = null, Action<Exception, string, object> onDbError = null)
        {
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("Connection string can not be null or empty.", nameof(connectionString));

            this.connectionString = connectionString;
            this.outputSql = outputSql;
            this.onDbError = onDbError;
        }

        public IEnumerable<T> Query<T>(PreparedQuery query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            return this.QueryIterator<T>(query);
        }

        public async Task<List<T>> QueryAsync<T>(PreparedQuery query, CancellationToken cancellationToken)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            var shaper = query.Compiled.GetShaper<T>();
            var command = this.CreateCommand(query, cancellationToken);
            var results = new List<T>();

            try
            {
                using (var connection = new SqlConnection(this.connectionString))
                using (var reader = await connection.ExecuteReaderAsync(command).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) results.Add(shaper(reader, query.ParameterValues));
                }
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                this.onDbError?.Invoke(ex, command.CommandText, command.Parameters);

                throw;
            }

            return results;
        }

        public bool HasRows(PreparedQuery query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            var command = this.CreateCommand(query, CancellationToken.None);

            using (var connection = new SqlConnection(this.connectionString))
            using (var reader = this.ExecuteReader(connection, command))
            {
                return reader.Read();
            }
        }

        public async Task<bool> HasRowsAsync(PreparedQuery query, CancellationToken cancellationToken)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            var command = this.CreateCommand(query, cancellationToken);

            try
            {
                using (var connection = new SqlConnection(this.connectionString))
                using (var reader = await connection.ExecuteReaderAsync(command).ConfigureAwait(false))
                {
                    return await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                this.onDbError?.Invoke(ex, command.CommandText, command.Parameters);

                throw;
            }
        }

        private IEnumerable<T> QueryIterator<T>(PreparedQuery query)
        {
            var shaper = query.Compiled.GetShaper<T>();
            var command = this.CreateCommand(query, CancellationToken.None);

            using (var connection = new SqlConnection(this.connectionString))
            using (var reader = this.ExecuteReader(connection, command))
            {
                while (reader.Read()) yield return shaper(reader, query.ParameterValues);
            }
        }

        /// <summary>
        ///     Dapper 會自動開啟關閉中的連線；讀取器一律是 <see cref="DbDataReader" />（SqlClient 的讀取器經 Dapper 包裝後仍是）。
        /// </summary>
        private DbDataReader ExecuteReader(SqlConnection connection, CommandDefinition command)
        {
            try
            {
                var reader = connection.ExecuteReader(command);

                return reader as DbDataReader ?? throw new InvalidOperationException($"Expected a DbDataReader but got '{reader.GetType()}'.");
            }
            catch (Exception ex)
            {
                this.onDbError?.Invoke(ex, command.CommandText, command.Parameters);

                throw;
            }
        }

        private CommandDefinition CreateCommand(PreparedQuery query, CancellationToken cancellationToken)
        {
            var parameters = SqlServerParameterMapper.ToDynamicParameters(query.Parameters);

            this.outputSql?.Invoke(query.Sql, parameters);

            return new CommandDefinition(query.Sql, parameters, cancellationToken: cancellationToken);
        }
    }
}
