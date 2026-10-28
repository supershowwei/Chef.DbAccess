using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlServer.Querying;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    /// <summary>
    ///     ChefQueryable / ChefQueryProvider 從 LINQ 到交給執行端的整段流程；執行端以假的 <see cref="IQueryExecutor" /> 代替資料庫。
    /// </summary>
    [TestClass]
    public class ChefQueryProviderTest
    {
        private readonly QueryCompiler compiler = new QueryCompiler(SqlServerDialect.Instance);
        private readonly FakeQueryExecutor executor = new FakeQueryExecutor();

        private IQueryable<Member> Members => new ChefQueryable<Member>(new ChefQueryProvider(this.compiler, this.executor));

        [TestMethod]
        public void Enumerating_executes_the_translated_sql()
        {
            var johnny = new Member { Id = 1, Name = "Johnny", Age = 20 };
            this.executor.Rows = new object[] { johnny };

            this.Members.Where(x => x.Age > 18).ToList().Should().Equal(johnny);

            var query = this.executor.Queries.Should().ContainSingle().Which;
            query.Sql.Should().EndWith("\nWHERE [mem_1].[Age] > {=Age_0}");
            query.Parameters.Single().Value.Should().Be(18);
        }

        [TestMethod]
        public void Operators_compose_into_chef_queryable()
        {
            var members = this.Members;

            var query = members.Where(x => x.Age > 18).OrderBy(x => x.Name);

            query.Should().BeOfType<ChefQueryable<Member>>();
            query.Provider.Should().BeSameAs(members.Provider);
        }

        [TestMethod]
        public void Query_syntax()
        {
            var query = from m in this.Members
                        where m.Age > 18
                        orderby m.Name
                        select new { m.Id, m.Name };

            query.ToQueryString()
                .Should()
                .Be(
                    "SELECT [mem_1].[No] AS [Id], [mem_1].[Name]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "WHERE [mem_1].[Age] > {=Age_0}\n" +
                    "ORDER BY [mem_1].[Name] ASC\n" +
                    "\n" +
                    "-- Parameters\n" +
                    "-- @Age_0 = 18 (Int32)");
        }

        [TestMethod]
        public void First_and_FirstOrDefault()
        {
            var johnny = new Member { Id = 1 };

            this.executor.Rows = new object[] { johnny };
            this.Members.First(x => x.Id == 1).Should().BeSameAs(johnny);
            this.executor.Queries.Last().Sql.Should().StartWith("SELECT TOP(1) ");

            this.executor.Rows = new object[0];
            this.Members.FirstOrDefault(x => x.Id == 2).Should().BeNull();

            Action first = () => this.Members.First(x => x.Id == 2);
            first.Should().Throw<InvalidOperationException>();
        }

        [TestMethod]
        public void Single_throws_when_more_than_one_row()
        {
            this.executor.Rows = new object[] { new Member(), new Member() };

            Action single = () => this.Members.Single(x => x.Age > 1);

            single.Should().Throw<InvalidOperationException>();
            this.executor.Queries.Single().Sql.Should().StartWith("SELECT TOP(2) ");
        }

        [TestMethod]
        public void Count_and_LongCount()
        {
            this.executor.Rows = new object[] { 3 };
            this.Members.Count(x => x.Age > 1).Should().Be(3);
            this.executor.Queries.Last().Sql.Should().StartWith("SELECT COUNT(*) ");

            this.executor.Rows = new object[] { 3L };
            this.Members.LongCount().Should().Be(3L);
            this.executor.Queries.Last().Sql.Should().StartWith("SELECT COUNT_BIG(*) ");
        }

        [TestMethod]
        public void Any_and_All()
        {
            this.executor.HasRowsResult = true;

            this.Members.Any(x => x.Age > 18).Should().BeTrue();
            this.Members.All(x => x.Age > 18).Should().BeFalse();
            this.executor.Queries.Last().Sql.Should().EndWith("WHERE NOT ([mem_1].[Age] > {=Age_0})");
        }

        [TestMethod]
        public void Queries_from_different_roots_share_the_cache()
        {
            var age = 18;

            this.Members.Where(x => x.Age > age).ToList();
            age = 30;
            this.Members.Where(x => x.Age > age).ToList();

            this.compiler.CacheCount.Should().Be(1);
            this.executor.Queries.Select(q => q.Parameters.Single().Value).Should().Equal(18, 30);
        }

        [TestMethod]
        public void Untranslatable_query_throws_when_enumerated()
        {
            Action act = () => this.Members.Where(x => x.Name.PadLeft(3) == "a").ToList();

            act.Should().Throw<NotSupportedException>();
            this.executor.Queries.Should().BeEmpty();
        }

        [TestMethod]
        public void Non_generic_CreateQuery()
        {
            var expression = this.Members.Where(x => x.Age > 1).Expression;

            ((IQueryProvider)new ChefQueryProvider(this.compiler, this.executor)).CreateQuery(expression).Should().BeOfType<ChefQueryable<Member>>();
        }

        [TestMethod]
        public async Task ToListAsync_and_ToArrayAsync()
        {
            var johnny = new Member { Id = 1 };
            this.executor.Rows = new object[] { johnny };

            (await this.Members.Where(x => x.Age > 18).ToListAsync()).Should().Equal(johnny);
            (await this.Members.ToArrayAsync()).Should().Equal(johnny);
            this.executor.Queries.First().Sql.Should().EndWith("\nWHERE [mem_1].[Age] > {=Age_0}");
        }

        [TestMethod]
        public async Task Async_result_operators_share_sql_with_sync_ones()
        {
            this.executor.Rows = new object[] { new Member { Id = 1 } };

            (await this.Members.FirstAsync(x => x.Id == 1)).Id.Should().Be(1);
            this.Members.First(x => x.Id == 1);

            this.executor.Queries.Select(q => q.Sql).Distinct().Should().ContainSingle().Which.Should().StartWith("SELECT TOP(1) ");
            this.compiler.CacheCount.Should().Be(1);
        }

        [TestMethod]
        public async Task Async_result_operators()
        {
            this.executor.Rows = new object[0];
            (await this.Members.FirstOrDefaultAsync()).Should().BeNull();
            (await this.Members.SingleOrDefaultAsync(x => x.Id == 2)).Should().BeNull();

            Func<Task> first = () => this.Members.FirstAsync();
            await first.Should().ThrowAsync<InvalidOperationException>();

            this.executor.Rows = new object[] { new Member(), new Member() };
            Func<Task> single = () => this.Members.SingleAsync(x => x.Age > 1);
            await single.Should().ThrowAsync<InvalidOperationException>();

            this.executor.Rows = new object[] { 3 };
            (await this.Members.CountAsync(x => x.Age > 1)).Should().Be(3);

            this.executor.Rows = new object[] { 3L };
            (await this.Members.LongCountAsync()).Should().Be(3L);

            this.executor.HasRowsResult = true;
            (await this.Members.AnyAsync()).Should().BeTrue();
            (await this.Members.AllAsync(x => x.Age > 1)).Should().BeFalse();
        }

        [TestMethod]
        public async Task CancellationToken_is_passed_to_executor()
        {
            using (var cancellation = new CancellationTokenSource())
            {
                await this.Members.ToListAsync(cancellation.Token);
                this.executor.LastCancellationToken.Should().Be(cancellation.Token);

                this.executor.HasRowsResult = true;
                await this.Members.AnyAsync(x => x.Age > 1, cancellation.Token);
                this.executor.LastCancellationToken.Should().Be(cancellation.Token);
            }
        }

        [TestMethod]
        public void Async_operators_require_chef_query()
        {
            Func<Task> act = () => new Member[0].AsQueryable().ToListAsync();

            act.Should().Throw<InvalidOperationException>();
        }

        [TestMethod]
        public void ToQueryString_requires_chef_query()
        {
            Action act = () => new Member[0].AsQueryable().ToQueryString();

            act.Should().Throw<InvalidOperationException>();
        }

        [TestMethod]
        public async Task Collection_include_merges_rows_for_sync_and_async()
        {
            // 執行端回傳 Shaper 組好的每一列：同一位會員出現兩次，各帶一個部門。
            Member Row(int id, params Department[] departments) => new Member { Id = id, Name = "M" + id, ManagedDepartments = departments.ToList() };

            this.executor.Rows = new object[]
                                 {
                                     Row(1, new Department { Id = 10 }),
                                     Row(2),
                                     Row(1, new Department { Id = 11 })
                                 };

            var query = this.Members.Include(m => m.ManagedDepartments, (m, d) => m.Id == d.ManagerId);

            foreach (var results in new[] { query.ToList(), await query.ToListAsync() })
            {
                results.Select(m => m.Id).Should().Equal(1, 2);
                results[0].ManagedDepartments.Select(d => d.Id).Should().Equal(10, 11);
                results[1].ManagedDepartments.Should().BeEmpty();
            }
        }

        [Table("tbl_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }

            public List<Department> ManagedDepartments { get; set; }
        }

        [Table("tbl_department")]
        private class Department
        {
            public int Id { get; set; }

            public int ManagerId { get; set; }
        }

        private sealed class FakeQueryExecutor : IQueryExecutor
        {
            public List<PreparedQuery> Queries { get; } = new List<PreparedQuery>();

            public object[] Rows { get; set; } = new object[0];

            public bool HasRowsResult { get; set; }

            public CancellationToken LastCancellationToken { get; private set; }

            public IEnumerable<T> Query<T>(PreparedQuery query)
            {
                this.Queries.Add(query);

                return this.Rows.Cast<T>();
            }

            public Task<List<T>> QueryAsync<T>(PreparedQuery query, CancellationToken cancellationToken)
            {
                this.LastCancellationToken = cancellationToken;

                return Task.FromResult(this.Query<T>(query).ToList());
            }

            public bool HasRows(PreparedQuery query)
            {
                this.Queries.Add(query);

                return this.HasRowsResult;
            }

            public Task<bool> HasRowsAsync(PreparedQuery query, CancellationToken cancellationToken)
            {
                this.LastCancellationToken = cancellationToken;

                return Task.FromResult(this.HasRows(query));
            }
        }
    }
}
