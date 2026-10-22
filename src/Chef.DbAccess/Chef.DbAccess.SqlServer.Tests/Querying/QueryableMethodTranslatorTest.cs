using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
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
        public void Entity_shaper_binds_each_column()
        {
            var shaper = TranslateQuery(Members).Shaper.Should().BeAssignableTo<MemberInitExpression>().Which;

            shaper.Type.Should().Be(typeof(Member));
            shaper.Bindings.Select(b => b.Member.Name).Should().Equal(nameof(Member.Id), nameof(Member.Name), nameof(Member.Age));
            shaper.Bindings.Cast<MemberAssignment>().Select(b => ProjectionIndex(b.Expression)).Should().Equal(0, 1, 2);
        }

        [TestMethod]
        public void Select_anonymous_type()
        {
            var translated = TranslateQuery(Members.Select(x => new { x.Id, N = x.Name }));

            new SqlServerSqlGenerator().Generate(translated.Select).Sql.Should().StartWith("SELECT [mem_1].[No] AS [Id], [mem_1].[Name] AS [N]\n");

            var shaper = translated.Shaper.Should().BeAssignableTo<NewExpression>().Which;
            shaper.Arguments.Select(ProjectionIndex).Should().Equal(0, 1);
        }

        [TestMethod]
        public void Select_dto_with_member_init()
        {
            var translated = TranslateQuery(Members.Select(x => new MemberDto { Key = x.Id, Title = x.Name }));

            new SqlServerSqlGenerator().Generate(translated.Select).Sql.Should().StartWith("SELECT [mem_1].[No] AS [Key], [mem_1].[Name] AS [Title]\n");
            translated.Shaper.Should().BeAssignableTo<MemberInitExpression>().Which.Type.Should().Be(typeof(MemberDto));
        }

        [TestMethod]
        public void Select_scalar()
        {
            Translate(Members.Select(x => x.Name)).Sql.Should().StartWith("SELECT [mem_1].[Name]\n");
            Translate(Members.Select(x => x.Name.Length)).Sql.Should().StartWith("SELECT LEN([mem_1].[Name]) AS [Value]\n");

            ProjectionIndex(TranslateQuery(Members.Select(x => x.Name)).Shaper).Should().Be(0);
        }

        [TestMethod]
        public void Select_predicate_becomes_case_when()
        {
            Translate(Members.Select(x => new { Adult = x.Age >= 18 }))
                .Sql.Should()
                .StartWith("SELECT CASE WHEN [mem_1].[Age] >= {=Age_0} THEN 1 ELSE 0 END AS [Adult]\n");
        }

        [TestMethod]
        public void Constants_and_closure_values_stay_on_client()
        {
            var tag = "vip";

            var translated = TranslateQuery(Members.Select(x => new { x.Id, Tag = tag, Fixed = 1 }));

            new SqlServerSqlGenerator().Generate(translated.Select).Sql.Should().StartWith("SELECT [mem_1].[No] AS [Id]\n");

            var shaper = (NewExpression)translated.Shaper;
            ProjectionIndex(shaper.Arguments[0]).Should().Be(0);
            shaper.Arguments[1].Should().BeAssignableTo<ParameterExpression>().Which.Name.Should().Be("__tag_0");
            shaper.Arguments[2].Should().BeAssignableTo<ConstantExpression>().Which.Value.Should().Be(1);
        }

        [TestMethod]
        public void Select_without_columns_still_selects_one_value()
        {
            Translate(Members.Select(x => 1)).Sql.Should().StartWith("SELECT 1 AS [Value]\n");
        }

        [TestMethod]
        public void Where_and_OrderBy_after_Select_map_back_to_columns()
        {
            Translate(Members.Select(x => new { x.Id, N = x.Name }).Where(a => a.N.StartsWith("J")).OrderBy(a => a.Id))
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[No] AS [Id], [mem_1].[Name] AS [N]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "WHERE [mem_1].[Name] LIKE (@Name_0 + '%')\n" +
                    "ORDER BY [mem_1].[No] ASC");
        }

        [TestMethod]
        public void Select_after_Select_is_composed()
        {
            Translate(Members.Select(x => new { x.Id, x.Name }).Select(a => new MemberDto { Key = a.Id })).Sql.Should().StartWith("SELECT [mem_1].[No] AS [Key]\n");
        }

        [TestMethod]
        public void Entity_inside_projection_and_duplicate_alias()
        {
            Translate(Members.Select(x => new { Member = x, x.Name }))
                .Sql.Should()
                .StartWith("SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [mem_1].[Age], [mem_1].[Name] AS [Name1]\n");
        }

        [TestMethod]
        public void Select_Skip_Take_matches_3x_projection()
        {
            // 對應 3.x Query_Skip_Take。
            Translate(Members.OrderBy(x => x.Id).Select(x => new { x.Name, x.Age }).Skip(20).Take(10))
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name], [mem_1].[Age]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "ORDER BY [mem_1].[No] ASC\n" +
                    "OFFSET {=Skip_0} ROWS\n" +
                    "FETCH NEXT {=Take_0} ROWS ONLY");
        }

        [TestMethod]
        public void Select_Distinct_matches_3x()
        {
            // 對應 3.x Query_Distinct（只差結尾分號）。
            Translate(Members.Where(x => x.Id > 0).Select(x => new { x.Name }).Distinct())
                .Sql.Should()
                .Be("SELECT DISTINCT [mem_1].[Name]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nWHERE [mem_1].[No] > {=Id_0}");
        }

        [TestMethod]
        public void Member_not_in_projection_can_not_be_used_later()
        {
            Action unassigned = () => Translate(Members.Select(x => new MemberDto { Key = x.Id }).Where(d => d.Title == "a"));
            Action constructorOnly = () => Translate(Members.Select(x => new Tuple<int>(x.Id)).Where(t => t.Item1 > 1));
            Action selectWithIndex = () => Translate(Members.Select((x, i) => x.Id + i));

            unassigned.Should().Throw<NotSupportedException>();
            constructorOnly.Should().Throw<NotSupportedException>();
            selectWithIndex.Should().Throw<NotSupportedException>();
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
            return new SqlServerSqlGenerator().Generate(TranslateQuery(query).Select);
        }

        private static TranslatedQuery TranslateQuery<T>(IQueryable<T> query)
        {
            var parameterized = PartialEvaluator.Evaluate(query.Expression);
            var translator = new QueryableMethodTranslator(
                new SqlTranslationContext(parameterized.ParameterValues),
                SqlServerTranslators.Members,
                SqlServerTranslators.MethodCalls);

            return translator.Translate(parameterized.Expression);
        }

        private static int ProjectionIndex(Expression expression)
        {
            return expression.Should().BeOfType<ProjectionBindingExpression>().Which.Index;
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

        private class MemberDto
        {
            public int Key { get; set; }

            public string Title { get; set; }
        }
    }
}
