using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
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
        public void ToQueryString_requires_chef_query()
        {
            Action act = () => new Member[0].AsQueryable().ToQueryString();

            act.Should().Throw<InvalidOperationException>();
        }

        [Table("tbl_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }

        private sealed class FakeQueryExecutor : IQueryExecutor
        {
            public List<PreparedQuery> Queries { get; } = new List<PreparedQuery>();

            public object[] Rows { get; set; } = new object[0];

            public bool HasRowsResult { get; set; }

            public IEnumerable<T> Query<T>(PreparedQuery query)
            {
                this.Queries.Add(query);

                return this.Rows.Cast<T>();
            }

            public bool HasRows(PreparedQuery query)
            {
                this.Queries.Add(query);

                return this.HasRowsResult;
            }
        }
    }
}
