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
    /// §10.2：條件＋分頁（第 6 頁，每頁 20 筆）。
    /// </summary>
    [MemoryDiagnoser]
    public class PagingBenchmark
    {
        private const int MinAge = 30;
        private const int Skip = 100;
        private const int Take = 20;

        private const string Sql = "SELECT " + BenchmarkDatabase.MemberColumns + @" FROM [Member] WITH (NOLOCK)
WHERE [IsActive] = 1 AND [Age] >= @MinAge
ORDER BY [CreatedAt]
OFFSET @Skip ROWS FETCH NEXT @Take ROWS ONLY";

        private IDataAccess<Member> dataAccess;

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
                return (await connection.QueryAsync<Member>(Sql, new { MinAge, Skip, Take })).AsList();
            }
        }

        [Benchmark]
        public Task<List<Member>> Chef3x()
        {
            var minAge = MinAge;

            return this.dataAccess.Where(x => x.IsActive && x.Age >= minAge)
                .OrderBy(x => x.CreatedAt)
                .Skip(Skip)
                .Take(Take)
                .Select(x => new { x.Id, x.Name, x.Email, x.Age, x.Balance, x.IsActive, x.CreatedAt, x.UpdatedAt, x.Token, x.DepartmentId })
                .QueryAsync();
        }

        [Benchmark]
        public Task<List<Member>> Chef3xRawSql()
        {
            return this.dataAccess.QueryAsync(Sql, new { MinAge, Skip, Take });
        }
    }
}
