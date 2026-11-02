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
    /// §10.2：三表 JOIN＋投影（一家公司底下的在職會員，約 500 筆）。
    /// </summary>
    [MemoryDiagnoser]
    public class JoinBenchmark
    {
        private const int CompanyId = 3;

        private const string Sql = @"SELECT [m].[Id], [m].[Name], [m].[Email], [d].[Id], [d].[Name], [c].[Id], [c].[Name]
FROM [Member] [m] WITH (NOLOCK)
INNER JOIN [Department] [d] WITH (NOLOCK) ON [m].[DepartmentId] = [d].[Id]
INNER JOIN [Company] [c] WITH (NOLOCK) ON [d].[CompanyId] = [c].[Id]
WHERE [c].[Id] = @CompanyId AND [m].[IsActive] = 1";

        private IDataAccess<Member> dataAccess;

        [GlobalSetup]
        public async Task Setup()
        {
            BenchmarkDatabase.EnsureCreated();

            this.dataAccess = SqlServerDataAccessFactory.Instance.Create<Member>();

            var expected = await this.Dapper();

            ResultCheck.AssertSame(nameof(this.Chef3x), expected, await this.Chef3x());
        }

        [Benchmark(Baseline = true)]
        public async Task<List<Member>> Dapper()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                var members = await connection.QueryAsync<Member, Department, Company, Member>(
                                  Sql,
                                  (member, department, company) =>
                                      {
                                          department.Company = company;
                                          member.Department = department;

                                          return member;
                                      },
                                  new { CompanyId },
                                  splitOn: "Id,Id");

                return members.AsList();
            }
        }

        [Benchmark]
        public Task<List<Member>> Chef3x()
        {
            var companyId = CompanyId;

            return this.dataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                .InnerJoin((x, y) => y.Company, (x, y, z) => y.CompanyId == z.Id)
                .Where((x, y, z) => z.Id == companyId && x.IsActive)
                .Select((x, y, z) => new { x.Id, x.Name, x.Email, DepartmentId = y.Id, DepartmentName = y.Name, CompanyId = z.Id, CompanyName = z.Name })
                .QueryAsync();
        }
    }
}
