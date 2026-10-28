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

        private static IQueryable<Department> Departments => new Department[0].AsQueryable();

        private static IQueryable<Employee> Employees => new Employee[0].AsQueryable();

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
        public void Query_is_a_sequence_by_default()
        {
            TranslateQuery(Members.Where(x => x.Age > 1)).ResultKind.Should().Be(QueryResultKind.Sequence);
        }

        [TestMethod]
        public void First_takes_one_row()
        {
            // 對應 3.x QueryOne_Where_Select；差異：加上 TOP(1)，3.x 會讀取所有符合的資料列再取第一筆。
            var translated = TranslateResult(Members.Where(x => x.Id == 1).Select(x => new { x.Name, x.Age }), q => q.First());

            Sql(translated).Should().Be("SELECT TOP(1) [mem_1].[Name], [mem_1].[Age]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nWHERE [mem_1].[No] = {=Id_0}");
            translated.ResultKind.Should().Be(QueryResultKind.First);
            translated.Shaper.Should().BeAssignableTo<NewExpression>();
        }

        [TestMethod]
        public void FirstOrDefault_with_predicate()
        {
            var translated = TranslateResult(Members, q => q.FirstOrDefault(x => x.Age > 18));

            Sql(translated).Should().StartWith("SELECT TOP(1) ").And.EndWith("\nWHERE [mem_1].[Age] > {=Age_0}");
            translated.ResultKind.Should().Be(QueryResultKind.FirstOrDefault);
        }

        [TestMethod]
        public void Single_takes_two_rows_to_detect_duplicates()
        {
            var single = TranslateResult(Members, q => q.Single(x => x.Id == 1));
            var singleOrDefault = TranslateResult(Members.Where(x => x.Id == 1), q => q.SingleOrDefault());

            Sql(single).Should().StartWith("SELECT TOP(2) ");
            single.ResultKind.Should().Be(QueryResultKind.Single);
            Sql(singleOrDefault).Should().StartWith("SELECT TOP(2) ");
            singleOrDefault.ResultKind.Should().Be(QueryResultKind.SingleOrDefault);
        }

        [TestMethod]
        public void Count()
        {
            // 對應 3.x Count；差異：COUNT(*) 多了別名、沒有分號。
            var translated = TranslateResult(Members.Where(x => x.Age == 20), q => q.Count());

            Sql(translated).Should().Be("SELECT COUNT(*) AS [Value]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nWHERE [mem_1].[Age] = {=Age_0}");
            translated.ResultKind.Should().Be(QueryResultKind.Scalar);
            translated.Shaper.Should().BeOfType<ProjectionBindingExpression>().Which.Type.Should().Be(typeof(int));
        }

        [TestMethod]
        public void Count_with_predicate_ignores_ordering_and_projection()
        {
            Sql(TranslateResult(Members.OrderBy(x => x.Name).Select(x => new { x.Name }), q => q.Count(a => a.Name == "J")))
                .Should()
                .Be("SELECT COUNT(*) AS [Value]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nWHERE [mem_1].[Name] = @Name_0");
        }

        [TestMethod]
        public void LongCount_uses_count_big()
        {
            var translated = TranslateResult(Members, q => q.LongCount());

            Sql(translated).Should().StartWith("SELECT COUNT_BIG(*) AS [Value]\n");
            translated.Shaper.Type.Should().Be(typeof(long));
        }

        [TestMethod]
        public void Any_takes_one_row()
        {
            // 3.x 的 Exists 輸出 CAST(CASE WHEN EXISTS (...) THEN 1 ELSE 0 END AS BIT)；這裡只取一筆看有沒有資料。
            var translated = TranslateResult(Members.Where(x => x.Id == 1), q => q.Any());

            Sql(translated).Should().Be("SELECT TOP(1) 1 AS [Value]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nWHERE [mem_1].[No] = {=Id_0}");
            translated.ResultKind.Should().Be(QueryResultKind.Any);
            translated.Shaper.Should().BeNull();

            Sql(TranslateResult(Members.OrderBy(x => x.Name), q => q.Any(x => x.Age > 1))).Should().EndWith("\nWHERE [mem_1].[Age] > {=Age_0}");
        }

        [TestMethod]
        public void All_looks_for_a_row_that_does_not_match()
        {
            var translated = TranslateResult(Members, q => q.All(x => x.Age > 18));

            Sql(translated).Should().Be("SELECT TOP(1) 1 AS [Value]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nWHERE NOT ([mem_1].[Age] > {=Age_0})");
            translated.ResultKind.Should().Be(QueryResultKind.All);
        }

        [TestMethod]
        public void Result_operators_that_need_subquery_are_not_supported()
        {
            Action firstAfterTake = () => TranslateResult(Members.Take(5), q => q.First());
            Action firstWithPredicateAfterTake = () => TranslateResult(Members.Take(5), q => q.First(x => x.Age > 1));
            Action countAfterSkip = () => TranslateResult(Members.OrderBy(x => x.Id).Skip(1), q => q.Count());
            Action countAfterDistinct = () => TranslateResult(Members.Select(x => x.Name).Distinct(), q => q.Count());
            Action anyAfterTake = () => TranslateResult(Members.Take(1), q => q.Any());

            firstAfterTake.Should().Throw<NotSupportedException>();
            firstWithPredicateAfterTake.Should().Throw<NotSupportedException>();
            countAfterSkip.Should().Throw<NotSupportedException>();
            countAfterDistinct.Should().Throw<NotSupportedException>();
            anyAfterTake.Should().Throw<NotSupportedException>();
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

        [TestMethod]
        public void Join_becomes_inner_join()
        {
            Translate(Members.Join(Departments, m => m.Id, d => d.ManagerId, (m, d) => new { m.Name, DepartmentName = d.Name }))
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name], [dep_2].[Name] AS [DepartmentName]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[No] = [dep_2].[ManagerId]");
        }

        [TestMethod]
        public void Query_syntax_join_with_where_and_orderby()
        {
            // 中間的 where 讓 C# 產生 new { m, d } 的透明識別項，之後的 t.d.Name 對應回 [dep_2].[Name]。
            var query = from m in Members
                        join d in Departments on m.Id equals d.ManagerId
                        where d.Name.Contains("業務") && m.Age > 30
                        orderby m.Name
                        select new { m.Name, DepartmentName = d.Name };

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name], [dep_2].[Name] AS [DepartmentName]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[No] = [dep_2].[ManagerId]\n" +
                    "WHERE ([dep_2].[Name] LIKE (('%' + @Name_0) + '%')) AND ([mem_1].[Age] > {=Age_0})\n" +
                    "ORDER BY [mem_1].[Name] ASC");
        }

        [TestMethod]
        public void Join_selecting_both_entities()
        {
            var translated = TranslateQuery(Members.Join(Departments, m => m.Id, d => d.ManagerId, (m, d) => new { m, d }));

            Sql(translated)
                .Should()
                .StartWith("SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [mem_1].[Age], [dep_2].[Id] AS [Id1], [dep_2].[Name] AS [Name1], [dep_2].[ManagerId]\n");

            var shaper = translated.Shaper.Should().BeAssignableTo<NewExpression>().Which;
            shaper.Arguments.Select(a => a.Type).Should().Equal(typeof(Member), typeof(Department));
            ((MemberInitExpression)shaper.Arguments[1]).Bindings.Cast<MemberAssignment>().Select(b => ProjectionIndex(b.Expression)).Should().Equal(3, 4, 5);
        }

        [TestMethod]
        public void Join_without_projection_of_entity_selects_the_entity()
        {
            Translate(Members.Join(Departments, m => m.Id, d => d.ManagerId, (m, d) => d))
                .Sql.Should()
                .StartWith("SELECT [dep_2].[Id], [dep_2].[Name], [dep_2].[ManagerId]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nINNER JOIN ");
        }

        [TestMethod]
        public void Join_with_composite_key()
        {
            var query = from m in Members
                        join d in Departments on new { m.Id, m.Name } equals new { Id = d.ManagerId, d.Name }
                        select d.Id;

            Translate(query).Sql.Should().EndWith("ON ([mem_1].[No] = [dep_2].[ManagerId]) AND ([mem_1].[Name] = [dep_2].[Name])");
        }

        [TestMethod]
        public void Where_on_joined_source_goes_into_on()
        {
            var generated = Translate(
                Members.Where(m => m.Age > 30)
                    .Join(Departments.Where(d => d.Name == "業務").OrderBy(d => d.Id), m => m.Id, d => d.ManagerId, (m, d) => m.Name));

            generated.Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON ([mem_1].[No] = [dep_2].[ManagerId]) AND ([dep_2].[Name] = @Name_0)\n" +
                    "WHERE [mem_1].[Age] > {=Age_0}");
            generated.Parameters.Select(p => p.Name).Should().Equal("Name_0", "Age_0");
        }

        [TestMethod]
        public void Join_after_select_and_multiple_joins()
        {
            // 沒有表數上限：Join 的結果可以再 Join，同一張表 Join 兩次時別名各自編號。
            var query = from a in Members.Select(x => new { x.Id, x.Name })
                        join d in Departments on a.Id equals d.ManagerId
                        join b in Members on d.Id equals b.Age
                        where b.Name != a.Name
                        select new { Manager = a.Name, Department = d.Name, Member = b.Name };

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name] AS [Manager], [dep_2].[Name] AS [Department], [mem_3].[Name] AS [Member]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[No] = [dep_2].[ManagerId]\n" +
                    "INNER JOIN [tbl_member] [mem_3] WITH (NOLOCK) ON [dep_2].[Id] = [mem_3].[Age]\n" +
                    "WHERE [mem_3].[Name] <> [mem_1].[Name]");
        }

        [TestMethod]
        public void Result_operators_after_join()
        {
            var joined = Members.Join(Departments, m => m.Id, d => d.ManagerId, (m, d) => new { m, d });

            Sql(TranslateResult(joined, q => q.Count(t => t.d.Name == "業務")))
                .Should()
                .Be(
                    "SELECT COUNT(*) AS [Value]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[No] = [dep_2].[ManagerId]\n" +
                    "WHERE [dep_2].[Name] = @Name_0");
            Sql(TranslateResult(joined.Select(t => t.m), q => q.First())).Should().StartWith("SELECT TOP(1) [mem_1].[No] AS [Id], [mem_1].[Name], [mem_1].[Age]\n");
        }

        [TestMethod]
        public void Join_that_needs_subquery_is_not_supported()
        {
            Action outerTake = () => Translate(Members.Take(5).Join(Departments, m => m.Id, d => d.ManagerId, (m, d) => d));
            Action outerDistinct = () => Translate(Members.Distinct().Join(Departments, m => m.Id, d => d.ManagerId, (m, d) => d));
            Action innerTake = () => Translate(Members.Join(Departments.Take(5), m => m.Id, d => d.ManagerId, (m, d) => d));
            Action innerDistinct = () => Translate(Members.Join(Departments.Distinct(), m => m.Id, d => d.ManagerId, (m, d) => d));
            Action innerJoined = () => Translate(
                Members.Join(Departments.Join(Members, d => d.Id, b => b.Age, (d, b) => d), m => m.Id, d => d.ManagerId, (m, d) => d));
            Action predicateKey = () => Translate(Members.Join(Departments, m => m.Age > 1, d => d.Id > 1, (m, d) => d));

            outerTake.Should().Throw<NotSupportedException>();
            outerDistinct.Should().Throw<NotSupportedException>();
            innerTake.Should().Throw<NotSupportedException>();
            innerDistinct.Should().Throw<NotSupportedException>();
            innerJoined.Should().Throw<NotSupportedException>();
            predicateKey.Should().Throw<NotSupportedException>();
        }

        [TestMethod]
        public void GroupJoin_with_DefaultIfEmpty_becomes_left_join()
        {
            var query = from m in Members
                        join d in Departments on m.Id equals d.ManagerId into g
                        from d in g.DefaultIfEmpty()
                        where m.Age > 30
                        orderby d.Name
                        select new { m.Name, DepartmentName = d.Name };

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name], [dep_2].[Name] AS [DepartmentName]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[No] = [dep_2].[ManagerId]\n" +
                    "WHERE [mem_1].[Age] > {=Age_0}\n" +
                    "ORDER BY [dep_2].[Name] ASC");
        }

        [TestMethod]
        public void Left_join_entity_is_null_checked_by_key_column()
        {
            var query = from m in Members
                        join d in Departments on m.Id equals d.ManagerId into g
                        from d in g.DefaultIfEmpty()
                        select new { m.Name, d };

            var translated = TranslateQuery(query);

            // 判斷用的 [dep_2].[ManagerId] 已經在 SELECT 清單中，不另外選出。
            Sql(translated).Should().StartWith("SELECT [mem_1].[Name], [dep_2].[Id], [dep_2].[Name] AS [Name1], [dep_2].[ManagerId]\n");

            var department = ((NewExpression)translated.Shaper).Arguments[1].Should().BeAssignableTo<ConditionalExpression>().Which;
            ProjectionIndex(((BinaryExpression)department.Test).Left).Should().Be(3);
            department.IfTrue.Should().BeAssignableTo<ConstantExpression>().Which.Value.Should().BeNull();
            department.IfFalse.Should().BeAssignableTo<MemberInitExpression>();
        }

        [TestMethod]
        public void Where_on_left_joined_source_stays_in_on()
        {
            // 放在 ON 只篩選右邊，左邊的資料不會減少。
            var query = from m in Members
                        join d in Departments.Where(x => x.Name == "業務") on m.Id equals d.ManagerId into g
                        from d in g.DefaultIfEmpty()
                        select m.Name;

            Translate(query)
                .Sql.Should()
                .EndWith("LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON ([mem_1].[No] = [dep_2].[ManagerId]) AND ([dep_2].[Name] = @Name_0)");
        }

        [TestMethod]
        public void GroupJoin_without_DefaultIfEmpty_is_inner_join()
        {
            var query = from m in Members
                        join d in Departments on m.Id equals d.ManagerId into g
                        from d in g
                        select d.Name;

            Translate(query).Sql.Should().Contain("\nINNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON ");
            TranslateQuery(query).Shaper.Should().BeOfType<ProjectionBindingExpression>();
        }

        [TestMethod]
        public void SelectMany_without_result_selector()
        {
            var query = Members.GroupJoin(Departments, m => m.Id, d => d.ManagerId, (m, g) => g).SelectMany(g => g.DefaultIfEmpty());

            var translated = TranslateQuery(query);

            Sql(translated).Should().StartWith("SELECT [dep_2].[Id], [dep_2].[Name], [dep_2].[ManagerId]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nLEFT JOIN ");
            translated.Shaper.Should().BeAssignableTo<ConditionalExpression>();
        }

        [TestMethod]
        public void Left_join_then_join()
        {
            var query = from m in Members
                        join d in Departments on m.Id equals d.ManagerId into g
                        from d in g.DefaultIfEmpty()
                        join b in Members on m.Age equals b.Age
                        select new { m.Name, Department = d.Name, Peer = b.Name };

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name], [dep_2].[Name] AS [Department], [mem_3].[Name] AS [Peer]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[No] = [dep_2].[ManagerId]\n" +
                    "INNER JOIN [tbl_member] [mem_3] WITH (NOLOCK) ON [mem_1].[Age] = [mem_3].[Age]");
        }

        [TestMethod]
        public void GroupJoin_that_needs_subquery_is_not_supported()
        {
            Action groupJoinOnly = () => Translate(Members.GroupJoin(Departments, m => m.Id, d => d.ManagerId, (m, g) => new { m.Name, Count = g.Count() }));
            Action filteredGroup = () => Translate(
                from m in Members
                join d in Departments on m.Id equals d.ManagerId into g
                from d in g.Where(x => x.Id > 1).DefaultIfEmpty()
                select d.Name);
            Action groupInResult = () => Translate(
                from m in Members
                join d in Departments on m.Id equals d.ManagerId into g
                from d in g.DefaultIfEmpty()
                select new { d.Name, Count = g.Count() });
            Action groupSelected = () => Translate(
                from m in Members
                join d in Departments on m.Id equals d.ManagerId into g
                from d in g.DefaultIfEmpty()
                where m.Age > 1
                select g);
            Action keyWithoutColumn = () => Translate(
                from m in Members
                join d in Departments on m.Id equals 1 into g
                from d in g.DefaultIfEmpty()
                select d.Name);

            groupJoinOnly.Should().Throw<NotSupportedException>();
            filteredGroup.Should().Throw<NotSupportedException>();
            groupInResult.Should().Throw<NotSupportedException>();
            groupSelected.Should().Throw<NotSupportedException>();
            keyWithoutColumn.Should().Throw<NotSupportedException>().WithMessage("*must contain a column*");
        }

        [TestMethod]
        public void Two_from_clauses_become_cross_join()
        {
            var query = from m in Members
                        from d in Departments
                        select new { m.Name, DepartmentName = d.Name };

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name], [dep_2].[Name] AS [DepartmentName]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "CROSS JOIN [tbl_department] [dep_2] WITH (NOLOCK)");
        }

        [TestMethod]
        public void Cross_join_with_where_on_collection_and_after()
        {
            // 展開的集合的 Where 放進 WHERE；之後的 where 可以同時參考兩邊，等同以 WHERE 結合的 Join。
            var query = from m in Members.Where(x => x.Age > 30)
                        from d in Departments.Where(x => x.Name == "業務")
                        where m.Id == d.ManagerId
                        select m.Name;

            var generated = Translate(query);

            generated.Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "CROSS JOIN [tbl_department] [dep_2] WITH (NOLOCK)\n" +
                    "WHERE (([mem_1].[Age] > {=Age_0}) AND ([dep_2].[Name] = @Name_0)) AND ([mem_1].[No] = [dep_2].[ManagerId])");
            generated.Parameters.Select(p => p.Value).Should().Equal(30, "業務");
        }

        [TestMethod]
        public void Cross_join_then_join_and_selecting_entities()
        {
            var query = from m in Members
                        from d in Departments
                        join b in Members on d.Id equals b.Age
                        select new { m, d, b.Name };

            var translated = TranslateQuery(query);

            Sql(translated)
                .Should()
                .EndWith(
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "CROSS JOIN [tbl_department] [dep_2] WITH (NOLOCK)\n" +
                    "INNER JOIN [tbl_member] [mem_3] WITH (NOLOCK) ON [dep_2].[Id] = [mem_3].[Age]");
            ((NewExpression)translated.Shaper).Arguments.Select(a => a.Type).Should().Equal(typeof(Member), typeof(Department), typeof(string));
        }

        [TestMethod]
        public void SelectMany_without_result_selector_is_cross_join()
        {
            Translate(Members.SelectMany(m => Departments))
                .Sql.Should()
                .Be("SELECT [dep_2].[Id], [dep_2].[Name], [dep_2].[ManagerId]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\nCROSS JOIN [tbl_department] [dep_2] WITH (NOLOCK)");
        }

        [TestMethod]
        public void SelectMany_that_needs_subquery_is_not_supported()
        {
            Action correlated = () => Translate(Members.SelectMany(m => Departments.Where(d => d.ManagerId == m.Id)));
            Action outerTake = () => Translate(Members.Take(5).SelectMany(m => Departments));
            Action innerTake = () => Translate(Members.SelectMany(m => Departments.Take(5)));
            Action innerJoined = () => Translate(Members.SelectMany(m => Departments.Join(Members, d => d.Id, b => b.Age, (d, b) => d)));
            Action notQuery = () => Translate(Members.SelectMany(m => new[] { 1, 2 }));
            Action withIndex = () => Translate(Members.SelectMany((m, i) => Departments));

            correlated.Should().Throw<NotSupportedException>().WithMessage("*depends on the outer element*");
            outerTake.Should().Throw<NotSupportedException>();
            innerTake.Should().Throw<NotSupportedException>();
            innerJoined.Should().Throw<NotSupportedException>();
            notQuery.Should().Throw<NotSupportedException>();
            withIndex.Should().Throw<NotSupportedException>();
        }

        [TestMethod]
        public void Navigation_in_where_becomes_left_join()
        {
            Translate(Employees.Where(e => e.Department.Name == "業務").Select(e => e.Name))
                .Sql.Should()
                .Be(
                    "SELECT [emp_1].[Name]\n" +
                    "FROM [tbl_employee] [emp_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [emp_1].[DepartmentId] = [dep_2].[Id]\n" +
                    "WHERE [dep_2].[Name] = @Name_0");
        }

        [TestMethod]
        public void Same_navigation_is_joined_once()
        {
            var query = Employees.Where(e => e.Department.Name != null)
                .OrderBy(e => e.Department.Name)
                .Select(e => new { e.Name, DepartmentName = e.Department.Name })
                .Where(a => a.DepartmentName.StartsWith("業"));

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [emp_1].[Name], [dep_2].[Name] AS [DepartmentName]\n" +
                    "FROM [tbl_employee] [emp_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [emp_1].[DepartmentId] = [dep_2].[Id]\n" +
                    "WHERE [dep_2].[Name] IS NOT NULL AND ([dep_2].[Name] LIKE (@Name_0 + '%'))\n" +
                    "ORDER BY [dep_2].[Name] ASC");
        }

        [TestMethod]
        public void Nested_and_self_referencing_navigations()
        {
            Translate(Employees.Select(e => new { e.Name, ManagerName = e.Manager.Name, DepartmentManager = e.Department.Manager.Name }))
                .Sql.Should()
                .Be(
                    "SELECT [emp_1].[Name], [emp_2].[Name] AS [ManagerName], [emp_4].[Name] AS [DepartmentManager]\n" +
                    "FROM [tbl_employee] [emp_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_employee] [emp_2] WITH (NOLOCK) ON [emp_1].[ManagerId] = [emp_2].[Id]\n" +
                    "LEFT JOIN [tbl_department] [dep_3] WITH (NOLOCK) ON [emp_1].[DepartmentId] = [dep_3].[Id]\n" +
                    "LEFT JOIN [tbl_employee] [emp_4] WITH (NOLOCK) ON [dep_3].[ManagerId] = [emp_4].[Id]");
        }

        [TestMethod]
        public void Selected_navigation_entity_is_null_checked()
        {
            var translated = TranslateQuery(Employees.Select(e => new { e.Name, e.Department }).Where(a => a.Department.Name == "業務"));

            Sql(translated)
                .Should()
                .Be(
                    "SELECT [emp_1].[Name], [dep_2].[Id], [dep_2].[Name] AS [Name1], [dep_2].[ManagerId]\n" +
                    "FROM [tbl_employee] [emp_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [emp_1].[DepartmentId] = [dep_2].[Id]\n" +
                    "WHERE [dep_2].[Name] = @Name_0");

            // 以 ON 條件中右邊的 [dep_2].[Id] 判斷有沒有資料。
            var department = ((NewExpression)translated.Shaper).Arguments[1].Should().BeAssignableTo<ConditionalExpression>().Which;
            ProjectionIndex(((BinaryExpression)department.Test).Left).Should().Be(1);
        }

        [TestMethod]
        public void Navigation_after_paging_and_in_result_operators()
        {
            // 多對一的 LEFT JOIN 不會增加筆數，可以接在 Take 之後。
            Translate(Employees.OrderBy(e => e.Id).Take(10).Select(e => e.Department.Name))
                .Sql.Should()
                .Be(
                    "SELECT TOP({=Take_0}) [dep_2].[Name]\n" +
                    "FROM [tbl_employee] [emp_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [emp_1].[DepartmentId] = [dep_2].[Id]\n" +
                    "ORDER BY [emp_1].[Id] ASC");

            Sql(TranslateResult(Employees, q => q.Count(e => e.Department.Name == "業務")))
                .Should()
                .Be(
                    "SELECT COUNT(*) AS [Value]\n" +
                    "FROM [tbl_employee] [emp_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [emp_1].[DepartmentId] = [dep_2].[Id]\n" +
                    "WHERE [dep_2].[Name] = @Name_0");
        }

        [TestMethod]
        public void Navigation_in_join_key_and_result_selector()
        {
            var query = from e in Employees
                        join m in Members on e.Department.ManagerId equals m.Id
                        select new { e.Name, Member = m.Name, ManagerName = e.Manager.Name };

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [emp_1].[Name], [mem_2].[Name] AS [Member], [emp_4].[Name] AS [ManagerName]\n" +
                    "FROM [tbl_employee] [emp_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_department] [dep_3] WITH (NOLOCK) ON [emp_1].[DepartmentId] = [dep_3].[Id]\n" +
                    "INNER JOIN [tbl_member] [mem_2] WITH (NOLOCK) ON [dep_3].[ManagerId] = [mem_2].[No]\n" +
                    "LEFT JOIN [tbl_employee] [emp_4] WITH (NOLOCK) ON [emp_1].[ManagerId] = [emp_4].[Id]");
        }

        [TestMethod]
        public void Navigation_in_left_join_result()
        {
            var query = from m in Members
                        join e in Employees on m.Id equals e.Id into g
                        from e in g.DefaultIfEmpty()
                        select new { m.Name, DepartmentName = e.Department.Name };

            Translate(query)
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[Name], [dep_3].[Name] AS [DepartmentName]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [tbl_employee] [emp_2] WITH (NOLOCK) ON [mem_1].[No] = [emp_2].[Id]\n" +
                    "LEFT JOIN [tbl_department] [dep_3] WITH (NOLOCK) ON [emp_2].[DepartmentId] = [dep_3].[Id]");
        }

        [TestMethod]
        public void Navigation_on_joined_side_key_is_not_supported()
        {
            Action innerKey = () => Translate(Members.Join(Employees, m => m.Id, e => e.Department.ManagerId, (m, e) => e.Name));

            innerKey.Should().Throw<NotSupportedException>();
        }

        private static GeneratedSql Translate<T>(IQueryable<T> query)
        {
            return new SqlServerSqlGenerator().Generate(TranslateQuery(query).Select);
        }

        private static TranslatedQuery TranslateQuery<T>(IQueryable<T> query)
        {
            return TranslateExpression(query.Expression);
        }

        /// <summary>
        ///     First、Count 等運算子會直接執行查詢，所以從 <c>q =&gt; q.First()</c> 取出呼叫，換上 <paramref name="source" /> 的運算式再翻譯。
        /// </summary>
        private static TranslatedQuery TranslateResult<T, TResult>(IQueryable<T> source, Expression<Func<IQueryable<T>, TResult>> resultOperator)
        {
            var methodCall = (MethodCallExpression)resultOperator.Body;

            return TranslateExpression(methodCall.Update(null, new[] { source.Expression }.Concat(methodCall.Arguments.Skip(1))));
        }

        private static string Sql(TranslatedQuery translated)
        {
            return new SqlServerSqlGenerator().Generate(translated.Select).Sql;
        }

        private static TranslatedQuery TranslateExpression(Expression query)
        {
            var parameterized = PartialEvaluator.Evaluate(query);
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

        [Table("tbl_department")]
        private class Department
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int ManagerId { get; set; }

            [ForeignKey(nameof(ManagerId))]
            public Employee Manager { get; set; }
        }

        [Table("tbl_employee")]
        private class Employee
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int DepartmentId { get; set; }

            [ForeignKey(nameof(DepartmentId))]
            public Department Department { get; set; }

            public int? ManagerId { get; set; }

            [ForeignKey(nameof(ManagerId))]
            public Employee Manager { get; set; }
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
