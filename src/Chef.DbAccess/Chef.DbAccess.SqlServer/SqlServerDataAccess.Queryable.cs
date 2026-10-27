using System.Linq;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlServer.Querying;

namespace Chef.DbAccess.SqlServer
{
    public partial class SqlServerDataAccess<T>
    {
        /// <summary>
        ///     4.0 的 <see cref="IQueryable{T}" /> 查詢入口，見 <see cref="IDataAccess{T}.Query" />。
        ///     <para>
        ///         取呼叫當下的 <see cref="IsDirtyRead" />（決定是否加上 <c>WITH (NOLOCK)</c>）、<see cref="OutputSql" /> 與 <see cref="OnDbError" />；
        ///         建立時指定的資料表名稱一樣有效。
        ///     </para>
        /// </summary>
        public IQueryable<T> Query()
        {
            var compiler = this.IsDirtyRead ? SqlServerQueryCompilers.WithNoLock : SqlServerQueryCompilers.WithoutNoLock;
            var executor = new SqlServerQueryExecutor(this.connectionString, this.OutputSql, this.OnDbError);

            return new ChefQueryable<T>(new ChefQueryProvider(compiler, executor), this.tableName);
        }
    }
}
