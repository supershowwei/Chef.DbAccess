using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.SqlGeneration;
using Chef.DbAccess.SqlServer.Querying;
using Chef.DbAccess.SqlServer.SqlGeneration;
using Chef.DbAccess.Syntax;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    /// <summary>
    ///     lambda 經過 PartialEvaluator 與翻譯器後，以 SQL Server 方言輸出 WHERE 條件。
    ///     參數命名與布林欄位的寫法對照 3.x 的 SqlServerDataAccess.StatementExtension.Test。
    /// </summary>
    [TestClass]
    public class SqlExpressionTranslatorTest
    {
        private static readonly EntityMetadata MemberEntity = EntityMetadata.For<Member>();

        [TestMethod]
        public void Comparison_with_literal_becomes_parameter_of_the_column()
        {
            var generated = Translate(x => x.Age > 30);

            Where(generated).Should().Be("[m].[Age] > {=Age_0}");
            Parameter(generated, "Age_0").Value.Should().Be(30);
            Parameter(generated, "Age_0").Property.Should().BeSameAs(MemberEntity.FindColumn(nameof(Member.Age)));
        }

        [TestMethod]
        public void String_closure_variable_carries_column_for_DbString_mapping()
        {
            var name = "Johnny";

            var generated = Translate(x => x.Name == name);

            Where(generated).Should().Be("[m].[first_name] = @Name_0");
            Parameter(generated, "Name_0").Value.Should().Be("Johnny");
            Parameter(generated, "Name_0").Property.ColumnTypeName.Should().Be("varchar");
        }

        [TestMethod]
        public void Value_on_the_left_side_still_maps_to_the_column()
        {
            var generated = Translate(x => 30 < x.Age);

            Where(generated).Should().Be("{=Age_0} < [m].[Age]");
            Parameter(generated, "Age_0").Property.PropertyName.Should().Be(nameof(Member.Age));
        }

        [TestMethod]
        public void And_or_are_parenthesized_and_parameters_are_numbered_per_property()
        {
            var generated = Translate(x => x.Age > 1 && (x.Age < 10 || x.Id == 1));

            Where(generated).Should().Be("([m].[Age] > {=Age_0}) AND (([m].[Age] < {=Age_1}) OR ([m].[No] = {=Id_0}))");
            Parameter(generated, "Age_1").Value.Should().Be(10);
        }

        [TestMethod]
        public void Comparison_between_columns()
        {
            Where(Translate(x => x.Age == x.Id)).Should().Be("[m].[Age] = [m].[No]");
        }

        [TestMethod]
        public void Comparison_with_null_becomes_is_null()
        {
            string name = null;

            Where(Translate(x => x.Name == null)).Should().Be("[m].[first_name] IS NULL");
            Where(Translate(x => null != x.Name)).Should().Be("[m].[first_name] IS NOT NULL");
            Where(Translate(x => x.Name == name)).Should().Be("[m].[first_name] IS NULL");
            Where(Translate(x => x.Level != null)).Should().Be("[m].[Level] IS NOT NULL");
        }

        [TestMethod]
        public void Boolean_column_as_predicate()
        {
            var generated = Translate(x => x.IsActive);

            Where(generated).Should().Be("[m].[IsActive] = {=IsActive_0}");
            Parameter(generated, "IsActive_0").Value.Should().Be(true);
        }

        [TestMethod]
        public void Negated_boolean_column_compares_with_false()
        {
            var generated = Translate(x => !x.IsActive && x.Age > 1);

            Where(generated).Should().Be("([m].[IsActive] = {=IsActive_0}) AND ([m].[Age] > {=Age_0})");
            Parameter(generated, "IsActive_0").Value.Should().Be(false);
        }

        [TestMethod]
        public void Negated_condition_becomes_not()
        {
            Where(Translate(x => !(x.Age > 30 || x.Id == 1))).Should().Be("NOT (([m].[Age] > {=Age_0}) OR ([m].[No] = {=Id_0}))");
        }

        [TestMethod]
        public void Boolean_closure_variable_and_constant_as_predicate()
        {
            var flag = true;

            Where(Translate(x => flag)).Should().Be("{=flag_0} = 1");
            Where(Translate(x => true)).Should().Be("1 = 1");
            Where(Translate(x => false)).Should().Be("1 = 0");
        }

        [TestMethod]
        public void Enum_comparison_uses_underlying_value()
        {
            var status = MemberStatus.Resigned;

            var generated = Translate(x => x.Status == MemberStatus.Active || x.Status == status);

            Where(generated).Should().Be("([m].[Status] = {=Status_0}) OR ([m].[Status] = {=Status_1})");
            Parameter(generated, "Status_0").Value.Should().Be(1);
            Parameter(generated, "Status_1").Value.Should().Be(2);
        }

        [TestMethod]
        public void Nullable_column_compares_with_value()
        {
            var generated = Translate(x => x.Level == 3);

            Where(generated).Should().Be("[m].[Level] = {=Level_0}");
            Parameter(generated, "Level_0").Value.Should().Be(3);
        }

        [TestMethod]
        public void Arithmetic_and_bitwise_values_borrow_the_column()
        {
            Where(Translate(x => x.Age + 1 > 30)).Should().Be("([m].[Age] + {=Age_0}) > {=Age_1}");
            Where(Translate(x => (x.Id & 1) == 1)).Should().Be("([m].[No] & {=Id_0}) = {=Id_1}");
            Where(Translate(x => -x.Age < 0)).Should().Be("-[m].[Age] < {=Age_0}");
        }

        [TestMethod]
        public void Decimal_conversion_with_operator_method_uses_the_column()
        {
            // int 轉 decimal 會帶著 Decimal.op_Implicit，decimal 轉 double 會帶著 Decimal.op_Explicit。
            Where(Translate(x => x.Price > x.Age)).Should().Be("[m].[Price] > [m].[Age]");

            var generated = Translate(x => (double)x.Price > 1.5);

            Where(generated).Should().Be("[m].[Price] > {=Price_0}");
            Parameter(generated, "Price_0").Value.Should().Be(1.5);
        }

        [TestMethod]
        public void Non_numeric_conversion_with_operator_method_throws()
        {
            var since = DateTimeOffset.Now;

            // DateTime 欄位轉 DateTimeOffset 會帶著 op_Implicit，牽涉時區，不靜默忽略。
            Action act = () => Translate(x => x.CreatedAt > since);

            // .NET Framework 的 Expression.ToString() 不會輸出轉型的目標型別。
            act.Should().Throw<NotSupportedException>().WithMessage("Can not translate 'Convert(x.CreatedAt*' in *");
        }

        [TestMethod]
        public void Nullable_HasValue_and_Value()
        {
            Where(Translate(x => x.Level.HasValue)).Should().Be("[m].[Level] IS NOT NULL");
            Where(Translate(x => !x.Level.HasValue)).Should().Be("[m].[Level] IS NULL");
            Where(Translate(x => x.Level.Value > 3)).Should().Be("[m].[Level] > {=Level_0}");
        }

        [TestMethod]
        public void Equals_method_is_equality()
        {
            string name = null;

            Where(Translate(x => x.Id.Equals(1))).Should().Be("[m].[No] = {=Id_0}");
            Where(Translate(x => !x.Id.Equals(1))).Should().Be("NOT ([m].[No] = {=Id_0})");
            Where(Translate(x => x.Name.Equals("J"))).Should().Be("[m].[first_name] = @Name_0");
            Where(Translate(x => string.Equals(x.Name, name))).Should().Be("[m].[first_name] IS NULL");
        }

        [TestMethod]
        public void Equals_with_boxed_enum_argument()
        {
            var generated = Translate(x => x.Status.Equals(MemberStatus.Active));

            Where(generated).Should().Be("[m].[Status] = {=Status_0}");
            Parameter(generated, "Status_0").Value.Should().Be(MemberStatus.Active);
        }

        [TestMethod]
        public void String_Contains_StartsWith_EndsWith_become_like()
        {
            var keyword = "oh";

            var generated = Translate(x => x.Name.Contains(keyword));

            // 對應 3.x 的 [first_name] LIKE '%' + @FirstName_0 + '%'；差異：串接多了括號。
            Where(generated).Should().Be("[m].[first_name] LIKE (('%' + @Name_0) + '%')");
            Parameter(generated, "Name_0").Value.Should().Be("oh");
            Parameter(generated, "Name_0").Property.ColumnTypeName.Should().Be("varchar");

            Where(Translate(x => x.Name.StartsWith("J"))).Should().Be("[m].[first_name] LIKE (@Name_0 + '%')");
            Where(Translate(x => x.Name.EndsWith("y"))).Should().Be("[m].[first_name] LIKE ('%' + @Name_0)");
        }

        [TestMethod]
        public void Like_wildcards_in_value_are_escaped_with_escape_clause()
        {
            // 差異：3.x 不跳脫，"50%" 會比對到所有含 50 的資料。
            var generated = Translate(x => x.Name.Contains("50%_[a]\\"));

            Where(generated).Should().Be("[m].[first_name] LIKE (('%' + @Name_0) + '%') ESCAPE '\\'");
            Parameter(generated, "Name_0").Value.Should().Be("50\\%\\_\\[a]\\\\");
        }

        [TestMethod]
        public void Like_null_value_stays_null_without_escape_clause()
        {
            string keyword = null;

            var generated = Translate(x => x.Name.StartsWith(keyword));

            Where(generated).Should().Be("[m].[first_name] LIKE (@Name_0 + '%')");
            Parameter(generated, "Name_0").Value.Should().BeNull();
        }

        [TestMethod]
        public void Negated_string_Contains()
        {
            Where(Translate(x => !x.Name.Contains("a"))).Should().Be("NOT ([m].[first_name] LIKE (('%' + @Name_0) + '%'))");
            Where(Translate(x => !x.Name.Contains("a_"))).Should().Be("NOT ([m].[first_name] LIKE (('%' + @Name_0) + '%') ESCAPE '\\')");
        }

        [TestMethod]
        public void Like_column_pattern_is_escaped_with_replace()
        {
            Where(Translate(x => "Johnny".Contains(x.Name)))
                .Should()
                .Be(
                    "@Name_0 LIKE (('%' + REPLACE(REPLACE(REPLACE(REPLACE([m].[first_name], '\\', '\\\\'), '%', '\\%'), '_', '\\_'), '[', '\\[')) + '%') ESCAPE '\\'");
        }

        [TestMethod]
        public void String_method_overload_with_comparison_throws()
        {
            Action act = () => Translate(x => x.Name.StartsWith("J", StringComparison.OrdinalIgnoreCase));

            act.Should().Throw<NotSupportedException>().WithMessage("Can not translate*StartsWith*");
        }

        [TestMethod]
        public void String_IsNullOrEmpty_uses_like_empty()
        {
            Where(Translate(x => string.IsNullOrEmpty(x.Name))).Should().Be("[m].[first_name] IS NULL OR ([m].[first_name] LIKE '')");
            Where(Translate(x => !string.IsNullOrEmpty(x.Name))).Should().Be("NOT ([m].[first_name] IS NULL OR ([m].[first_name] LIKE ''))");
        }

        [TestMethod]
        public void String_Length_becomes_len()
        {
            var generated = Translate(x => x.Name.Length > 3);

            // LEN 的結果不是字串，比較的值不沿用 varchar 欄位。
            Where(generated).Should().Be("LEN([m].[first_name]) > {=p_0}");
            Parameter(generated, "p_0").Property.Should().BeNull();
        }

        [TestMethod]
        public void String_ToUpper_ToLower_Trim()
        {
            var generated = Translate(x => x.Name.ToUpper() == "JOHNNY");

            // 回傳字串的函式沿用欄位，varchar 欄位一樣送成 DbString。
            Where(generated).Should().Be("UPPER([m].[first_name]) = @Name_0");
            Parameter(generated, "Name_0").Property.ColumnTypeName.Should().Be("varchar");

            Where(Translate(x => x.Name.ToLower() == "johnny")).Should().Be("LOWER([m].[first_name]) = @Name_0");
            Where(Translate(x => x.Name.Trim() == "johnny")).Should().Be("LTRIM(RTRIM([m].[first_name])) = @Name_0");
        }

        [TestMethod]
        public void String_Substring_is_one_based()
        {
            var start = 2;

            Where(Translate(x => x.Name.Substring(1, 2) == "oh")).Should().Be("SUBSTRING([m].[first_name], {=p_0} + 1, {=p_1}) = @Name_0");
            Where(Translate(x => x.Name.Substring(start) == "hnny")).Should().Be("SUBSTRING([m].[first_name], {=start_0} + 1, LEN([m].[first_name])) = @Name_0");
        }

        [TestMethod]
        public void String_method_overload_with_culture_throws()
        {
            Action act = () => Translate(x => x.Name.ToUpper(CultureInfo.InvariantCulture) == "A");

            act.Should().Throw<NotSupportedException>().WithMessage("Can not translate*ToUpper*");
        }

        [TestMethod]
        public void String_functions_need_dialect_translators()
        {
            Action act = () => Translate(x => x.Name.Length > 3, withSqlServerTranslators: false);

            act.Should().Throw<NotSupportedException>().WithMessage("Can not translate 'x.Name.Length'*");
        }

        [TestMethod]
        public void DateTime_parts_become_datepart()
        {
            Where(Translate(x => x.CreatedAt.Year == 2020)).Should().Be("DATEPART(year, [m].[CreatedAt]) = {=p_0}");
            Where(Translate(x => x.CreatedAt.Month == 1 && x.CreatedAt.Hour > 8))
                .Should()
                .Be("(DATEPART(month, [m].[CreatedAt]) = {=p_0}) AND (DATEPART(hour, [m].[CreatedAt]) > {=p_1})");
            Where(Translate(x => x.UpdatedAt.Day == 1)).Should().Be("DATEPART(day, [m].[UpdatedAt]) = {=p_0}");
        }

        [TestMethod]
        public void DateTime_Date_becomes_convert_to_date()
        {
            var today = DateTime.Today;

            var generated = Translate(x => x.CreatedAt.Date == today);

            // CONVERT(date, ...) 的結果是日期，比較的值沿用 CreatedAt 欄位。
            Where(generated).Should().Be("CONVERT(date, [m].[CreatedAt]) = @CreatedAt_0");
            Parameter(generated, "CreatedAt_0").Value.Should().Be(today);
        }

        [TestMethod]
        public void DateTime_add_methods_become_dateadd()
        {
            var now = DateTime.Now;

            Where(Translate(x => x.CreatedAt.AddMonths(-1) > now)).Should().Be("DATEADD(month, {=p_0}, [m].[CreatedAt]) > @CreatedAt_0");
            Where(Translate(x => x.CreatedAt.AddYears(1) > now)).Should().Be("DATEADD(year, {=p_0}, [m].[CreatedAt]) > @CreatedAt_0");
        }

        [TestMethod]
        public void DateTime_add_methods_keep_fraction_as_milliseconds()
        {
            var now = DateTime.Now;

            // DATEADD 會捨去小數，所以整數部分用原本的單位，小數部分換算成毫秒；已在 LocalDB 確認 1.5、-1.5、0.1、-0.25 天的結果與 C# 相同。
            var generated = Translate(x => x.CreatedAt.AddDays(1.5) > now);

            Where(generated)
                .Should()
                .Be("DATEADD(millisecond, ROUND(({=p_0} - FLOOR({=p_0})) * 86400000, 0), DATEADD(day, FLOOR({=p_0}), [m].[CreatedAt])) > @CreatedAt_0");
            Parameter(generated, "p_0").Value.Should().Be(1.5);

            Where(Translate(x => x.CreatedAt.AddHours(1.5) > now))
                .Should()
                .Be("DATEADD(millisecond, ROUND(({=p_0} - FLOOR({=p_0})) * 3600000, 0), DATEADD(hour, FLOOR({=p_0}), [m].[CreatedAt])) > @CreatedAt_0");

            Where(Translate(x => x.CreatedAt.AddMilliseconds(1.5) > now)).Should().Be("DATEADD(millisecond, ROUND({=p_0}, 0), [m].[CreatedAt]) > @CreatedAt_0");
        }

        [TestMethod]
        public void Math_functions()
        {
            Where(Translate(x => Math.Abs(x.Age) > 3)).Should().Be("ABS([m].[Age]) > {=Age_0}");
            Where(Translate(x => Math.Floor(x.Price) == 1)).Should().Be("FLOOR([m].[Price]) = {=Price_0}");
            Where(Translate(x => Math.Ceiling(x.Price) == 1)).Should().Be("CEILING([m].[Price]) = {=Price_0}");
            Where(Translate(x => Math.Round(x.Price) == 1)).Should().Be("ROUND([m].[Price], 0) = {=Price_0}");
            Where(Translate(x => Math.Round(x.Price, 2) == 1.23m)).Should().Be("ROUND([m].[Price], {=p_0}) = {=Price_0}");
        }

        [TestMethod]
        public void Math_Round_with_midpoint_rounding_throws()
        {
            Action act = () => Translate(x => Math.Round(x.Price, MidpointRounding.AwayFromZero) == 1);

            act.Should().Throw<NotSupportedException>().WithMessage("Can not translate*Round*");
        }

        [TestMethod]
        public void Conditional_becomes_case_when()
        {
            var generated = Translate(x => (x.Age > 18 ? x.Name : "minor") == "adult");

            // 分支與比較的值都沿用 Name 欄位，varchar 欄位一樣送成 DbString。
            Where(generated).Should().Be("CASE WHEN [m].[Age] > {=Age_0} THEN [m].[first_name] ELSE @Name_0 END = @Name_1");
            Parameter(generated, "Name_0").Property.ColumnTypeName.Should().Be("varchar");
            Parameter(generated, "Name_1").Value.Should().Be("adult");

            Where(Translate(x => (x.Age > 18 ? 1 : 0) == 1)).Should().Be("CASE WHEN [m].[Age] > {=Age_0} THEN {=p_0} ELSE {=p_1} END = {=p_2}");
        }

        [TestMethod]
        public void Conditional_with_predicate_branches_in_condition_becomes_and_or()
        {
            Where(Translate(x => x.IsActive ? x.Age > 18 : x.Age > 21))
                .Should()
                .Be(
                    "(([m].[IsActive] = {=IsActive_0}) AND ([m].[Age] > {=Age_0})) OR " +
                    "(NOT ([m].[IsActive] = {=IsActive_0}) AND ([m].[Age] > {=Age_1}))");
        }

        [TestMethod]
        public void Conditional_with_predicate_branches_as_value_throws()
        {
            Action act = () => Translate(x => (x.IsActive ? x.Age > 18 : x.Age > 21) == true);

            act.Should().Throw<NotSupportedException>().WithMessage("Can not translate*");
        }

        [TestMethod]
        public void Coalesce_becomes_coalesce()
        {
            var generated = Translate(x => (x.Name ?? "-") == "-");

            Where(generated).Should().Be("COALESCE([m].[first_name], @Name_0) = @Name_1");
            Parameter(generated, "Name_0").Property.ColumnTypeName.Should().Be("varchar");

            Where(Translate(x => (x.Level ?? 0) > 3)).Should().Be("COALESCE([m].[Level], {=Level_0}) > {=Level_1}");
        }

        [TestMethod]
        public void Collection_Contains_becomes_in()
        {
            var ids = new[] { 1, 2, 3 };
            var ages = new List<int> { 10, 20 };

            // 對應 3.x 的 [Id] = {=Id_0} OR [Id] = {=Id_1} OR [Id] = {=Id_2}。
            var generated = Translate(x => ids.Contains(x.Id));

            Where(generated).Should().Be("[m].[No] IN ({=Id_0}, {=Id_1}, {=Id_2})");
            Parameter(generated, "Id_2").Value.Should().Be(3);

            Where(Translate(x => ages.Contains(x.Age))).Should().Be("[m].[Age] IN ({=Age_0}, {=Age_1})");
        }

        [TestMethod]
        public void Literal_string_array_Contains_carries_column()
        {
            var generated = Translate(x => new[] { "a", "b" }.Contains(x.Name));

            Where(generated).Should().Be("[m].[first_name] IN (@Name_0, @Name_1)");
            Parameter(generated, "Name_1").Property.ColumnTypeName.Should().Be("varchar");
        }

        [TestMethod]
        public void Negated_collection_Contains_becomes_not_in()
        {
            var ids = new[] { 1, 2 };

            Where(Translate(x => !ids.Contains(x.Id))).Should().Be("[m].[No] NOT IN ({=Id_0}, {=Id_1})");
        }

        [TestMethod]
        public void Empty_collection_Contains()
        {
            var ids = new List<int>();

            // 與 3.x 相同。
            Where(Translate(x => ids.Contains(x.Id))).Should().Be("1 = 0");
            Where(Translate(x => !ids.Contains(x.Id))).Should().Be("1 = 1");
        }

        [TestMethod]
        public void Null_element_in_collection_adds_is_null()
        {
            var names = new[] { "a", null };
            var onlyNull = new string[] { null };

            Where(Translate(x => names.Contains(x.Name))).Should().Be("[m].[first_name] IN (@Name_0) OR [m].[first_name] IS NULL");
            Where(Translate(x => !names.Contains(x.Name))).Should().Be("NOT ([m].[first_name] IN (@Name_0) OR [m].[first_name] IS NULL)");
            Where(Translate(x => onlyNull.Contains(x.Name))).Should().Be("[m].[first_name] IS NULL");
        }

        [TestMethod]
        public void Enum_collection_Contains()
        {
            var statuses = new[] { MemberStatus.Active };

            var generated = Translate(x => statuses.Contains(x.Status));

            Where(generated).Should().Be("[m].[Status] IN ({=Status_0})");
            Parameter(generated, "Status_0").Value.Should().Be(MemberStatus.Active);
        }

#if NET5_0_OR_GREATER
        // C# 14 起，ids.Contains(x.Id) 會綁定成 MemoryExtensions.Contains(ReadOnlySpan<int>.op_Implicit(ids), x.Id)。
        // 這裡手動組出該形狀，讓測試不受編譯器版本影響。
        [TestMethod]
        public void Span_Contains_from_csharp_14_becomes_in()
        {
            var ids = new[] { 1, 2 };
            Expression<Func<int[]>> idsExpr = () => ids;

            var x = Expression.Parameter(typeof(Member), "x");
            var span = Expression.Call(typeof(ReadOnlySpan<int>).GetMethod("op_Implicit", new[] { typeof(int[]) }), idsExpr.Body);
            var contains = typeof(MemoryExtensions).GetMethods()
                .Single(
                    m => m.Name == nameof(MemoryExtensions.Contains) && m.IsGenericMethodDefinition && m.GetParameters().Length == 2
                         && m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>))
                .MakeGenericMethod(typeof(int));
            var predicate = Expression.Lambda<Func<Member, bool>>(Expression.Call(contains, span, Expression.Property(x, nameof(Member.Id))), x);

            Where(Translate(predicate)).Should().Be("[m].[No] IN ({=Id_0}, {=Id_1})");
        }
#endif

        [TestMethod]
        public void Full_text_Includes_becomes_contains()
        {
            var generated = Translate(x => x.Name.Includes("Johnny"));

            // 與 3.x 相同：CONTAINS([first_name], @FirstName_0)。
            Where(generated).Should().Be("CONTAINS([m].[first_name], @Name_0)");
            Parameter(generated, "Name_0").Property.ColumnTypeName.Should().Be("varchar");

            Where(Translate(x => !x.Name.Includes("J") && x.Age > 1)).Should().Be("NOT (CONTAINS([m].[first_name], @Name_0)) AND ([m].[Age] > {=Age_0})");
        }

        [TestMethod]
        public void Evaluated_client_value_becomes_parameter()
        {
            var generated = Translate(x => x.CreatedAt > DateTime.Today.AddDays(-7));

            Where(generated).Should().Be("[m].[CreatedAt] > @CreatedAt_0");
            Parameter(generated, "CreatedAt_0").Value.Should().Be(DateTime.Today.AddDays(-7));
        }

        [TestMethod]
        public void Unmapped_property_throws_with_expression()
        {
            Action act = () => Translate(x => x.Secret == "a");

            act.Should().Throw<NotSupportedException>().WithMessage("*x.Secret*not mapped*");
        }

        [TestMethod]
        public void Untranslatable_expression_throws_with_expression()
        {
            Action act = () => Translate(x => x.Name.PadLeft(3) == "a");

            act.Should().Throw<NotSupportedException>().WithMessage("Can not translate 'x.Name.PadLeft(3)' in 'x => (x.Name.PadLeft(3) == \"a\")' to SQL.");
        }

        [TestMethod]
        public void Lambda_parameters_must_match_tables()
        {
            Expression<Func<Member, bool>> predicate = x => x.Age > 1;
            var translator = new SqlExpressionTranslator(new SqlTranslationContext(new Dictionary<string, object>()));

            Action act = () => translator.TranslatePredicate(predicate);

            act.Should().Throw<ArgumentException>();
        }

        private static GeneratedSql Translate(Expression<Func<Member, bool>> predicate, bool withSqlServerTranslators = true)
        {
            var query = PartialEvaluator.Evaluate(predicate);
            var table = TableExpression.For(MemberEntity, "m");
            var translator = withSqlServerTranslators
                                 ? new SqlExpressionTranslator(
                                     new SqlTranslationContext(query.ParameterValues),
                                     SqlServerTranslators.Members,
                                     SqlServerTranslators.MethodCalls)
                                 : new SqlExpressionTranslator(new SqlTranslationContext(query.ParameterValues));

            var condition = translator.TranslatePredicate((LambdaExpression)query.Expression, table);

            var select = new SelectExpression(
                new[] { new ProjectionExpression(table.Column(MemberEntity.FindColumn(nameof(Member.Id))), nameof(Member.Id)) },
                new[] { table },
                condition);

            return new SqlServerSqlGenerator(withNoLock: false).Generate(select);
        }

        private static string Where(GeneratedSql generated)
        {
            const string Keyword = "\nWHERE ";

            return generated.Sql.Substring(generated.Sql.IndexOf(Keyword, StringComparison.Ordinal) + Keyword.Length);
        }

        private static SqlParameterExpression Parameter(GeneratedSql generated, string name)
        {
            return generated.Parameters.Single(p => p.Name == name);
        }

        private enum MemberStatus
        {
            Active = 1,
            Resigned = 2
        }

        [Table("tbl_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            [Column("first_name", TypeName = "varchar")]
            [StringLength(20)]
            public string Name { get; set; }

            public int Age { get; set; }

            public decimal Price { get; set; }

            public int? Level { get; set; }

            public bool IsActive { get; set; }

            public MemberStatus Status { get; set; }

            public DateTime CreatedAt { get; set; }

            public DateTimeOffset UpdatedAt { get; set; }

            [NotMapped]
            public string Secret { get; set; }
        }
    }
}
