using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Chef.DbAccess.Benchmarks.Database;
using Chef.DbAccess.SqlServer;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.Benchmarks.Write
{
    /// <summary>
    /// §10.2：新增一筆（9 個欄位，Id 由資料庫產生）。
    /// 量測期間資料會累積（每個寫法約數萬筆），新資料都接在 IDENTITY 主鍵的尾端，對時間的影響很小。
    /// </summary>
    [MemoryDiagnoser]
    public class InsertBenchmark
    {
        private IDataAccess<MemberWrite> dataAccess;

        // 每次都寫入同一個物件，每次呼叫的成本相同
        private MemberWrite member;

        [GlobalSetup]
        public async Task Setup()
        {
            BenchmarkDatabase.EnsureCreated();

            this.dataAccess = SqlServerDataAccessFactory.Instance.Create<MemberWrite>();
            this.member = MemberWriteTable.CreateRows(1)[0];

            await this.Check(nameof(this.Dapper), this.Dapper);
            await this.Check(nameof(this.Chef3x), this.Chef3x);
            await this.Check(nameof(this.Chef3xRawSql), this.Chef3xRawSql);

            MemberWriteTable.Truncate();
        }

        [Benchmark(Baseline = true)]
        public async Task<int> Dapper()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                return await connection.ExecuteAsync(MemberWriteTable.InsertSql, this.member);
            }
        }

        [Benchmark]
        public Task<int> Chef3x()
        {
            return this.dataAccess.InsertAsync(this.member);
        }

        [Benchmark]
        public Task<int> Chef3xRawSql()
        {
            return this.dataAccess.ExecuteAsync(MemberWriteTable.InsertSql, this.member);
        }

        // 從空的資料表開始，確認這個寫法新增了一筆，而且每個欄位都與來源相同
        private async Task Check(string name, Func<Task<int>> insert)
        {
            MemberWriteTable.Truncate();

            var affected = await insert();

            if (affected != 1) throw new InvalidOperationException($"{name}：影響筆數是 {affected}，預期 1");

            ResultCheck.AssertSame(name, new[] { this.member }, MemberWriteTable.ReadAll());
        }
    }
}
