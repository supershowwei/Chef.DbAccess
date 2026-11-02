using System.Collections.Generic;
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
    /// §10.2：查 1,000 筆、1 萬筆 × 10 欄。
    /// </summary>
    [MemoryDiagnoser]
    public class QueryManyBenchmark
    {
        private const string Sql = "SELECT " + BenchmarkDatabase.MemberColumns + " FROM [Member] WITH (NOLOCK) WHERE [Id] <= @Count";

        private IDataAccess<Member> dataAccess;

        [Params(1_000, 10_000)]
        public int Count { get; set; }

        [GlobalSetup]
        public async Task Setup()
        {
            BenchmarkDatabase.EnsureCreated();

            this.dataAccess = SqlServerDataAccessFactory.Instance.Create<Member>();

            var expected = await this.Dapper();

            ResultCheck.AssertSame(nameof(this.Chef3x), expected, await this.Chef3x());
            ResultCheck.AssertSame(nameof(this.Chef3xRawSql), expected, await this.Chef3xRawSql());
        }

        [Benchmark(Baseline = true)]
        public async Task<List<Member>> Dapper()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                return (await connection.QueryAsync<Member>(Sql, new { this.Count })).AsList();
            }
        }

        [Benchmark]
        public Task<List<Member>> Chef3x()
        {
            var count = this.Count;

            return this.dataAccess.Where(x => x.Id <= count)
                .Select(x => new { x.Id, x.Name, x.Email, x.Age, x.Balance, x.IsActive, x.CreatedAt, x.UpdatedAt, x.Token, x.DepartmentId })
                .QueryAsync();
        }

        [Benchmark]
        public Task<List<Member>> Chef3xRawSql()
        {
            return this.dataAccess.QueryAsync(Sql, new { this.Count });
        }
    }
}
