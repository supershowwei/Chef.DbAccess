using System;
using System.ComponentModel.DataAnnotations;
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
    ///     快取命中時，用第一次翻譯記錄的 ParameterBindings 與新的查詢參數值重新算出 SQL 參數，結果要和重新翻譯一次完全相同。
    /// </summary>
    [TestClass]
    public class ParameterBindingTest
    {
        private static IQueryable<Member> Members => new Member[0].AsQueryable();

        [TestMethod]
        public void Closure_variable_takes_new_value_and_literal_stays()
        {
            IQueryable<Member> Build(int age) => Members.Where(x => x.Age > age && x.Name == "Johnny");

            ShouldRebind(Build(30).Expression, Build(40).Expression);
        }

        [TestMethod]
        public void Boolean_column_condition_stays()
        {
            IQueryable<Member> Build(int age) => Members.Where(x => !x.IsActive && x.Age > age);

            ShouldRebind(Build(30).Expression, Build(40).Expression);
        }

        [TestMethod]
        public void Collection_elements_take_new_values()
        {
            IQueryable<Member> BuildIds(int[] ids) => Members.Where(x => ids.Contains(x.Id));
            IQueryable<Member> BuildNames(string[] names) => Members.Where(x => names.Contains(x.Name));

            ShouldRebind(BuildIds(new[] { 1, 2 }).Expression, BuildIds(new[] { 3, 4 }).Expression);
            ShouldRebind(BuildNames(new[] { "a", null, "b" }).Expression, BuildNames(new[] { "c", null, "d" }).Expression);
        }

        [TestMethod]
        public void Like_value_is_escaped_again()
        {
            IQueryable<Member> Build(string keyword) => Members.Where(x => x.Name.Contains(keyword));

            ShouldRebind(Build("50%").Expression, Build("a_b").Expression);
            ShouldRebind(Build("oh").Expression, Build("hn").Expression);
        }

        [TestMethod]
        public void Skip_and_Take_take_new_values()
        {
            IQueryable<Member> Build(int skip) => Members.OrderBy(x => x.Id).Skip(skip).Take(10);

            ShouldRebind(Build(20).Expression, Build(40).Expression);
        }

        [TestMethod]
        public void Result_operator_with_predicate()
        {
            Expression Build(int age)
            {
                Expression<Func<IQueryable<Member>, Member>> first = q => q.FirstOrDefault(x => x.Age > age);
                var methodCall = (MethodCallExpression)first.Body;

                return methodCall.Update(null, new[] { Members.Expression }.Concat(methodCall.Arguments.Skip(1)));
            }

            ShouldRebind(Build(30), Build(40));
        }

        private static void ShouldRebind(Expression first, Expression second)
        {
            var firstQuery = PartialEvaluator.Evaluate(first);
            var secondQuery = PartialEvaluator.Evaluate(second);

            // 前提：兩次執行會命中同一個快取項目。
            new QueryCacheKey(firstQuery).Should().Be(new QueryCacheKey(secondQuery));

            var firstTranslated = Translate(firstQuery);
            var firstSql = Generate(firstTranslated);
            var secondSql = Generate(Translate(secondQuery));

            firstSql.Sql.Should().Be(secondSql.Sql);

            var rebound = firstSql.Parameters.ToDictionary(p => p.Name, p => firstTranslated.ParameterBindings[p.Name](secondQuery.ParameterValues));

            rebound.Should().BeEquivalentTo(secondSql.Parameters.ToDictionary(p => p.Name, p => p.Value));
        }

        private static TranslatedQuery Translate(ParameterizedQuery query)
        {
            return new QueryableMethodTranslator(new SqlTranslationContext(query.ParameterValues), SqlServerTranslators.Members, SqlServerTranslators.MethodCalls)
                .Translate(query.Expression);
        }

        private static GeneratedSql Generate(TranslatedQuery translated)
        {
            return new SqlServerSqlGenerator().Generate(translated.Select);
        }

        [Table("tbl_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            [Column(TypeName = "varchar")]
            [StringLength(20)]
            public string Name { get; set; }

            public int Age { get; set; }

            public bool IsActive { get; set; }
        }
    }
}
