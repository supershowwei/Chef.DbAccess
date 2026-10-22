using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlServer.Querying;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    [TestClass]
    public class QueryCompilerTest
    {
        private readonly QueryCompiler compiler = new QueryCompiler(SqlServerDialect.Instance);

        private static IQueryable<Member> Members => new Member[0].AsQueryable();

        [TestMethod]
        public void Prepare_generates_sql_and_parameters()
        {
            var prepared = this.compiler.Prepare(ByAge(30).Expression);

            prepared.Sql.Should()
                .Be("SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [mem_1].[Age]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nWHERE [mem_1].[Age] > {=Age_0}");
            prepared.Parameters.Should().ContainSingle().Which.Value.Should().Be(30);
            prepared.Compiled.ResultKind.Should().Be(QueryResultKind.Sequence);
            prepared.Compiled.GetShaper<Member>().Should().NotBeNull();
        }

        [TestMethod]
        public void Same_shape_hits_cache_and_takes_new_values()
        {
            var first = this.compiler.Prepare(ByAge(30).Expression);
            var second = this.compiler.Prepare(ByAge(40).Expression);

            this.compiler.CacheCount.Should().Be(1);
            second.Compiled.Should().BeSameAs(first.Compiled);
            first.Parameters.Single().Value.Should().Be(30);
            second.Parameters.Single().Value.Should().Be(40);
        }

        [TestMethod]
        public void Different_shape_or_null_value_adds_cache_entry()
        {
            this.compiler.Prepare(ByName("Johnny").Expression);
            this.compiler.Prepare(ByName("Mary").Expression);
            this.compiler.Prepare(ByName(null).Expression).Sql.Should().EndWith("WHERE [mem_1].[Name] IS NULL");
            this.compiler.Prepare(ByAge(30).Expression);

            this.compiler.CacheCount.Should().Be(3);
        }

        [TestMethod]
        public void Collection_and_like_values_are_rebound()
        {
            IQueryable<Member> ByIds(int?[] ids) => Members.Where(x => ids.Contains(x.Id));
            IQueryable<Member> ByKeyword(string keyword) => Members.Where(x => x.Name.Contains(keyword));

            this.compiler.Prepare(ByIds(new int?[] { 1, null, 2 }).Expression);
            this.compiler.Prepare(ByIds(new int?[] { 3, null, 4 }).Expression).Parameters.Select(p => p.Value).Should().Equal(3, 4);

            this.compiler.Prepare(ByKeyword("50%").Expression);
            this.compiler.Prepare(ByKeyword("a_b").Expression).Parameters.Single().Value.Should().Be("a\\_b");

            this.compiler.CacheCount.Should().Be(2);
        }

        [TestMethod]
        public void Parameters_keep_column_for_type_mapping()
        {
            this.compiler.Prepare(ByName("Johnny").Expression).Parameters.Single().Property.ColumnTypeName.Should().Be("varchar");
        }

        [TestMethod]
        public void Any_has_no_shaper()
        {
            Expression<Func<IQueryable<Member>, bool>> any = q => q.Any(x => x.Age > 18);
            var methodCall = (MethodCallExpression)any.Body;

            var prepared = this.compiler.Prepare(methodCall.Update(null, new[] { Members.Expression }.Concat(methodCall.Arguments.Skip(1))));

            prepared.Compiled.ResultKind.Should().Be(QueryResultKind.Any);
            prepared.Compiled.Shaper.Should().BeNull();
        }

        [TestMethod]
        public void ToQueryString_lists_parameter_values()
        {
            var age = 30;

            this.compiler.Prepare(Members.Where(x => x.Age > age && x.Name.StartsWith("J")).Expression)
                .ToQueryString()
                .Should()
                .Be(
                    "SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [mem_1].[Age]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "WHERE ([mem_1].[Age] > {=Age_0}) AND ([mem_1].[Name] LIKE (@Name_0 + '%'))\n" +
                    "\n" +
                    "-- Parameters\n" +
                    "-- @Age_0 = 30 (Int32)\n" +
                    "-- @Name_0 = 'J' (String)");
        }

        [TestMethod]
        public void Untranslatable_query_throws_and_is_not_cached()
        {
            Action act = () => this.compiler.Prepare(Members.Where(x => x.Name.PadLeft(3) == "a").Expression);

            act.Should().Throw<NotSupportedException>();
            this.compiler.CacheCount.Should().Be(0);
        }

        private static IQueryable<Member> ByAge(int age)
        {
            return Members.Where(x => x.Age > age);
        }

        private static IQueryable<Member> ByName(string name)
        {
            return Members.Where(x => x.Name == name);
        }

        [Table("tbl_member")]
        private class Member
        {
            [Column("No")]
            public int? Id { get; set; }

            [Column(TypeName = "varchar")]
            [StringLength(20)]
            public string Name { get; set; }

            public int Age { get; set; }
        }
    }
}
