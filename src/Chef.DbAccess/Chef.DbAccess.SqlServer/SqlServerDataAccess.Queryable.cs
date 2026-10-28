using System;
using System.Collections.Generic;
using System.Linq;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlServer.Querying;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.SqlServer
{
    public partial class SqlServerDataAccess<T>
    {
        private (string Server, string Database)? queryRootSource;

        /// <summary>
        ///     4.0 的 <see cref="IQueryable{T}" /> 查詢入口，見 <see cref="IDataAccess{T}.Query" />。
        ///     <para>
        ///         取呼叫當下的 <see cref="IsDirtyRead" />（決定是否加上 <c>WITH (NOLOCK)</c>）、<see cref="OutputSql" /> 與 <see cref="OnDbError" />；
        ///         建立時指定的資料表名稱一樣有效。連線字串的伺服器與資料庫用來檢查 Join 的來源，見 <see cref="GetQueryRootSource" />。
        ///     </para>
        /// </summary>
        public IQueryable<T> Query()
        {
            var compiler = this.IsDirtyRead ? SqlServerQueryCompilers.WithNoLock : SqlServerQueryCompilers.WithoutNoLock;
            var executor = new SqlServerQueryExecutor(this.connectionString, this.OutputSql, this.OnDbError);
            // 連線字串不會變，解析一次就好；同時呼叫時頂多多解析一次。
            if (this.queryRootSource == null) this.queryRootSource = GetQueryRootSource(this.connectionString);

            var (server, database) = this.queryRootSource.Value;

            return new ChefQueryable<T>(new ChefQueryProvider(compiler, executor), this.tableName, server, database);
        }

        /// <summary>
        ///     連線字串的 <c>Data Source</c> 與 <c>Initial Catalog</c>（也接受 <c>Server</c>、<c>Database</c> 等別名）。
        ///     3.x 以 Regex 解析，<c>Initial Catalog</c> 是最後一個設定、後面沒有分號時會取不到；這裡改用 <see cref="SqlConnectionStringBuilder" />。
        ///     連線字串無法解析時回傳 <c>null</c>（視為不知道），由執行時的連線回報錯誤。
        /// </summary>
        internal static (string Server, string Database) GetQueryRootSource(string connectionString)
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString);

                return (builder.DataSource, builder.InitialCatalog);
            }
            catch (Exception ex) when (ex is ArgumentException || ex is FormatException || ex is KeyNotFoundException)
            {
                return (null, null);
            }
        }
    }
}
