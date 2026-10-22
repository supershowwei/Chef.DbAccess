using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlGeneration;
using Chef.DbAccess.SqlServer.Querying;
using Chef.DbAccess.SqlServer.SqlGeneration;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    /// <summary>
    ///     LINQ 查詢經過 PartialEvaluator 與 QueryableMethodTranslator 後，以 SQL Server 方言輸出。
    ///     對照 3.x 的特徵化快照（Characterization/Snapshots/QueryCharacterizationTest）；差異：沒有 Select 時選出所有欄位、結尾沒有分號。
    /// </summary>
    [TestClass]
    public class QueryableMethodTranslatorTest
    {
        private const string SelectAll = "SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [mem_1].[Age]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)";

        private static IQueryable<Member> Members => new Member[0].AsQueryable();

        [TestMethod]
        public void Query_root_selects_all_mapped_columns()
        {
            Translate(Members).Sql.Should().Be(SelectAll);
        }

        [TestMethod]
        public void Multiple_where_are_combined_with_and()
        {
            // 差異：3.x 的第二個 Where 會覆蓋第一個（Pitfall_Second_Where_Overwrites_First）。
            Translate(Members.Where(x => x.Age > 18).Where(x => x.Name == "Johnny"))
                .Sql.Should()
                .Be(SelectAll + "\nWHERE ([mem_1].[Age] > {=Age_0}) AND ([mem_1].[Name] = @Name_0)");
        }

        [TestMethod]
        public void Parameters_are_numbered_across_operators()
        {
            var generated = Translate(Members.Where(x => x.Age > 1).Where(x => x.Age < 9));

            generated.Sql.Should().EndWith("\nWHERE ([mem_1].[Age] > {=Age_0}) AND ([mem_1].[Age] < {=Age_1})");
            generated.Parameters.Select(p => p.Value).Should().Equal(1, 9);
        }

        [TestMethod]
        public void OrderBy_ThenByDescending()
        {
            // 對應 3.x Query_OrderBy_ThenByDescending。
            Translate(Members.Where(x => x.Age > 0).OrderBy(x => x.Id).ThenByDescending(x => x.Name))
                .Sql.Should()
                .Be(SelectAll + "\nWHERE [mem_1].[Age] > {=Age_0}\nORDER BY [mem_1].[No] ASC, [mem_1].[Name] DESC");
        }

        [TestMethod]
        public void Second_OrderBy_replaces_previous_ordering()
        {
            Translate(Members.OrderBy(x => x.Name).OrderByDescending(x => x.Age)).Sql.Should().Be(SelectAll + "\nORDER BY [mem_1].[Age] DESC");
        }

        [TestMethod]
        public void Skip_Take()
        {
            // 對應 3.x Query_Skip_Take；差異：數字是 {=} 參數，執行時由 Dapper 寫入，送出的 SQL 與 3.x 的 OFFSET 20 ROWS 相同。
            var generated = Translate(Members.OrderBy(x => x.Id).Skip(20).Take(10));

            generated.Sql.Should().Be(SelectAll + "\nORDER BY [mem_1].[No] ASC\nOFFSET {=Skip_0} ROWS\nFETCH NEXT {=Take_0} ROWS ONLY");
            generated.Parameters.Select(p => p.Value).Should().Equal(20, 10);
        }

        [TestMethod]
        public void Where_OrderBy_Take_uses_top()
        {
            // 對應 3.x Query_StartsWith_OrderBy_Take；差異：LIKE 右邊的串接多一層括號、TOP 的數字是 {=} 參數。
            Translate(Members.Where(x => x.Age >= 30 && x.Name.StartsWith("J")).OrderBy(x => x.Age).Take(10))
                .Sql.Should()
                .Be(
                    "SELECT TOP({=Take_0}) [mem_1].[No] AS [Id], [mem_1].[Name], [mem_1].[Age]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "WHERE ([mem_1].[Age] >= {=Age_0}) AND ([mem_1].[Name] LIKE (@Name_0 + '%'))\n" +
                    "ORDER BY [mem_1].[Age] ASC");
        }

        [TestMethod]
        public void Skip_Take_with_variables_give_same_sql_as_literals()
        {
            var skip = 40;
            var pageSize = 20;

            var generated = Translate(Members.OrderBy(x => x.Id).Skip(skip).Take(pageSize));

            generated.Sql.Should().Be(Translate(Members.OrderBy(x => x.Id).Skip(20).Take(10)).Sql);
            generated.Parameters.Select(p => p.Value).Should().Equal(40, 20);
        }

        [TestMethod]
        public void Distinct()
        {
            Translate(Members.Where(x => x.Id > 0).Distinct()).Sql.Should().StartWith("SELECT DISTINCT [mem_1].[No] AS [Id], ");
        }

        [TestMethod]
        public void Alias_follows_3x_naming()
        {
            Translate(new MemberProfile[0].AsQueryable()).Sql.Should().Be("SELECT [mp_1].[Id]\nFROM [MemberProfile] [mp_1] WITH (NOLOCK)");
        }

        [TestMethod]
        public void Operators_after_paging_are_not_supported()
        {
            Action whereAfterTake = () => Translate(Members.Take(10).Where(x => x.Age > 1));
            Action orderAfterSkip = () => Translate(Members.OrderBy(x => x.Id).Skip(10).OrderBy(x => x.Age));
            Action takeAfterTake = () => Translate(Members.Take(10).Take(5));
            Action skipAfterTake = () => Translate(Members.Take(10).Skip(5));

            whereAfterTake.Should().Throw<NotSupportedException>().WithMessage("*Where*after Skip or Take*");
            orderAfterSkip.Should().Throw<NotSupportedException>().WithMessage("*OrderBy*after Skip or Take*");
            takeAfterTake.Should().Throw<NotSupportedException>();
            skipAfterTake.Should().Throw<NotSupportedException>();
        }

        [TestMethod]
        public void Unsupported_operators_throw()
        {
            Action orderByPredicate = () => Translate(Members.OrderBy(x => x.Age > 18));
            Action whereWithIndex = () => Translate(Members.Where((x, i) => i > 1));
            Action reverse = () => Translate(Members.Reverse());

            orderByPredicate.Should().Throw<NotSupportedException>();
            whereWithIndex.Should().Throw<NotSupportedException>();
            reverse.Should().Throw<NotSupportedException>().WithMessage("Can not translate*Reverse*");
        }

        private static GeneratedSql Translate<T>(IQueryable<T> query)
        {
            var parameterized = PartialEvaluator.Evaluate(query.Expression);
            var translator = new QueryableMethodTranslator(
                new SqlTranslationContext(parameterized.ParameterValues),
                SqlServerTranslators.Members,
                SqlServerTranslators.MethodCalls);

            return new SqlServerSqlGenerator().Generate(translator.Translate(parameterized.Expression));
        }

        [Table("tbl_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }

        private class MemberProfile
        {
            public int Id { get; set; }
        }
    }
}
