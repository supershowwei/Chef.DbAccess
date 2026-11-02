using System;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Chef.DbAccess.Benchmarks.Database;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.Benchmarks.Write
{
    /// <summary>
    /// §10.2：依主鍵更新一筆的兩個欄位。每次都寫入同樣的值，不需要在量測之間重置資料。
    /// </summary>
    [MemoryDiagnoser]
    public class UpdateBenchmark
    {
        private const decimal Balance = 12_345.67m;

        private const string Sql = "UPDATE [MemberWrite] SET [Balance] = @Balance, [UpdatedAt] = @UpdatedAt WHERE [Id] = @Id";

        private static readonly DateTime UpdatedAt = new DateTime(2026, 6, 1);

        private IDataAccess<MemberWrite> dataAccess;

        private int id;

        [GlobalSetup]
        public async Task Setup()
        {
            BenchmarkDatabase.EnsureCreated();

            this.dataAccess = SqlServerDataAccessFactory.Instance.Create<MemberWrite>();

            MemberWriteTable.Truncate();

            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                this.id = connection.QuerySingle<int>(
                    MemberWriteTable.InsertSql + "; SELECT CAST(SCOPE_IDENTITY() AS INT)",
                    MemberWriteTable.CreateRows(1)[0]);
            }

            await this.Check(nameof(this.Dapper), this.Dapper);
            await this.Check(nameof(this.Chef3x), this.Chef3x);
            await this.Check(nameof(this.Chef3xRawSql), this.Chef3xRawSql);
        }

        [Benchmark(Baseline = true)]
        public async Task<int> Dapper()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                return await connection.ExecuteAsync(Sql, new { Balance, UpdatedAt, Id = this.id });
            }
        }

        [Benchmark]
        public Task<int> Chef3x()
        {
            var id = this.id;
            var balance = Balance;
            DateTime? updatedAt = UpdatedAt;

            return this.dataAccess.Where(x => x.Id == id).Set(() => new MemberWrite { Balance = balance, UpdatedAt = updatedAt }).UpdateAsync();
        }

        [Benchmark]
        public Task<int> Chef3xRawSql()
        {
            return this.dataAccess.ExecuteAsync(Sql, new { Balance, UpdatedAt, Id = this.id });
        }

        // 先把資料改成別的值，再確認這個寫法只影響一筆，而且改成了預期的值
        private async Task Check(string name, Func<Task<int>> update)
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                connection.Execute("UPDATE [MemberWrite] SET [Balance] = 0, [UpdatedAt] = NULL WHERE [Id] = @Id", new { Id = this.id });
            }

            var affected = await update();

            if (affected != 1) throw new InvalidOperationException($"{name}：影響筆數是 {affected}，預期 1");

            var row = MemberWriteTable.ReadAll().Single(x => x.Id == this.id);

            if (row.Balance != Balance || row.UpdatedAt != UpdatedAt)
            {
                throw new InvalidOperationException($"{name}：更新後的值不對（Balance {row.Balance}、UpdatedAt {row.UpdatedAt:O}）");
            }
        }
    }
}
