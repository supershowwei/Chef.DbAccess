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
    /// §10.2：主鍵查一筆。
    /// </summary>
    [MemoryDiagnoser]
    public class QueryOneBenchmark
    {
        private const int Id = 5_000;

        private const string Sql = "SELECT " + BenchmarkDatabase.MemberColumns + " FROM [Member] WITH (NOLOCK) WHERE [Id] = @Id";

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
        public async Task<Member> Dapper()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                return await connection.QueryFirstOrDefaultAsync<Member>(Sql, new { Id });
            }
        }

        [Benchmark]
        public Task<Member> Chef3x()
        {
            var id = Id;

            return this.dataAccess.Where(x => x.Id == id)
                .Select(x => new { x.Id, x.Name, x.Email, x.Age, x.Balance, x.IsActive, x.CreatedAt, x.UpdatedAt, x.Token, x.DepartmentId })
                .QueryOneAsync();
        }

        [Benchmark]
        public Task<Member> Chef3xRawSql()
        {
            return this.dataAccess.QueryOneAsync(Sql, new { Id });
        }
    }
}
