using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Chef.DbAccess.Benchmarks.Database;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.Benchmarks.Query
{
    /// <summary>
    /// §10.2：Contains 10／1,000 筆。3.x 翻成 OR 串並內嵌數值；Dapper 的 IN @Ids 每個值展開成一個參數。
    /// 1 萬筆兩邊都無法執行：3.x 內嵌的數值一樣會以參數送出，兩者都超過 SQL Server 一次 2,100 個參數的上限。
    /// 5.0 的 #暫存資料表寫法（§5.8）在 Phase 3 加入，到時再補上 1 萬筆。
    /// </summary>
    [MemoryDiagnoser]
    public class ContainsBenchmark
    {
        private const string Sql = "SELECT " + BenchmarkDatabase.MemberColumns + " FROM [Member] WITH (NOLOCK) WHERE [Id] IN @Ids";

        private IDataAccess<Member> dataAccess;

        private List<int> ids;

        [Params(10, 1_000)]
        public int Count { get; set; }

        [GlobalSetup]
        public async Task Setup()
        {
            BenchmarkDatabase.EnsureCreated();

            this.dataAccess = SqlServerDataAccessFactory.Instance.Create<Member>();
            this.ids = CreateIds(this.Count);

            var expected = await this.Dapper();

            ResultCheck.AssertSame(nameof(this.Chef3x), expected, await this.Chef3x());
            ResultCheck.AssertSame(nameof(this.Chef3xRawSql), expected, await this.Chef3xRawSql());
        }

        [Benchmark(Baseline = true)]
        public async Task<List<Member>> Dapper()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                return (await connection.QueryAsync<Member>(Sql, new { Ids = this.ids })).AsList();
            }
        }

        [Benchmark]
        public Task<List<Member>> Chef3x()
        {
            var values = this.ids;

            return this.dataAccess.Where(x => values.Contains(x.Id))
                .Select(x => new { x.Id, x.Name, x.Email, x.Age, x.Balance, x.IsActive, x.CreatedAt, x.UpdatedAt, x.Token, x.DepartmentId })
                .QueryAsync();
        }

        [Benchmark]
        public Task<List<Member>> Chef3xRawSql()
        {
            return this.dataAccess.QueryAsync(Sql, new { Ids = this.ids });
        }

        // 在 1～1 萬之間平均取樣
        private static List<int> CreateIds(int count)
        {
            return Enumerable.Range(0, count).Select(i => i * (BenchmarkDatabase.MemberCount / count) + 1).ToList();
        }
    }
}
