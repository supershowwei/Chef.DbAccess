using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Chef.DbAccess.Querying;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    [TestClass]
    public class ExpressionEqualityComparerTest
    {
        private static readonly ExpressionEqualityComparer Comparer = ExpressionEqualityComparer.Instance;

        [TestMethod]
        public void Same_lambda_built_twice_is_equal()
        {
            Expression<Func<Member, bool>> Build() => x => x.Age > 30 && x.Name.StartsWith("J");

            ShouldBeEqual(Build(), Build());
        }

        [TestMethod]
        public void Lambda_parameters_are_compared_by_position_not_name()
        {
            Expression<Func<Member, bool>> first = x => x.Age > 30;
            Expression<Func<Member, bool>> second = y => y.Age > 30;

            ShouldBeEqual(first, second);
        }

        [TestMethod]
        public void Lambda_parameters_at_different_positions_are_not_equal()
        {
            Expression<Func<int, int, bool>> first = (x, y) => x > 1;
            Expression<Func<int, int, bool>> second = (x, y) => y > 1;

            Comparer.Equals(first, second).Should().BeFalse();
        }

        [TestMethod]
        public void Nested_lambda_parameters_are_compared_by_position()
        {
            var ids = new List<int> { 1, 2 };

            Expression<Func<int, bool>> first = x => ids.Any(i => i == x);
            Expression<Func<int, bool>> second = y => ids.Any(j => j == y);
            Expression<Func<int, bool>> third = x => ids.Any(i => x == i);

            ShouldBeEqual(PartialEvaluator.Evaluate(first).Expression, PartialEvaluator.Evaluate(second).Expression);
            Comparer.Equals(PartialEvaluator.Evaluate(first).Expression, PartialEvaluator.Evaluate(third).Expression).Should().BeFalse();
        }

        [TestMethod]
        public void Free_parameters_are_compared_by_name()
        {
            var age0 = Expression.Parameter(typeof(int), "__age_0");

            ShouldBeEqual(age0, Expression.Parameter(typeof(int), "__age_0"));
            Comparer.Equals(age0, Expression.Parameter(typeof(int), "__age_1")).Should().BeFalse();
            Comparer.Equals(age0, Expression.Parameter(typeof(long), "__age_0")).Should().BeFalse();
        }

        [TestMethod]
        public void Different_constants_members_or_methods_are_not_equal()
        {
            Expression<Func<Member, bool>> baseline = x => x.Age > 30 && x.Name.StartsWith("J");
            Expression<Func<Member, bool>> constant = x => x.Age > 31 && x.Name.StartsWith("J");
            Expression<Func<Member, bool>> member = x => x.Id > 30 && x.Name.StartsWith("J");
            Expression<Func<Member, bool>> method = x => x.Age > 30 && x.Name.EndsWith("J");
            Expression<Func<Member, bool>> @operator = x => x.Age >= 30 && x.Name.StartsWith("J");

            Comparer.Equals(baseline, constant).Should().BeFalse();
            Comparer.Equals(baseline, member).Should().BeFalse();
            Comparer.Equals(baseline, method).Should().BeFalse();
            Comparer.Equals(baseline, @operator).Should().BeFalse();
        }

        [TestMethod]
        public void Folded_array_constants_are_compared_by_content()
        {
            Expression<Func<Member, bool>> Build(int second) => x => new[] { 1, second }.Contains(x.Id);
            Expression<Func<Member, bool>> BuildLiteral() => x => new[] { 1, 2 }.Contains(x.Id);

            ShouldBeEqual(PartialEvaluator.Evaluate(BuildLiteral()).Expression, PartialEvaluator.Evaluate(BuildLiteral()).Expression);

            // 含閉包變數的陣列會變成參數，值不影響形狀。
            ShouldBeEqual(PartialEvaluator.Evaluate(Build(2)).Expression, PartialEvaluator.Evaluate(Build(3)).Expression);

            var one = Expression.Constant(new[] { 1, 2 });
            ShouldBeEqual(one, Expression.Constant(new[] { 1, 2 }));
            Comparer.Equals(one, Expression.Constant(new[] { 1, 3 })).Should().BeFalse();
        }

        [TestMethod]
        public void Query_roots_are_compared_by_element_type()
        {
            var age = 30;

            Expression Build() => new Member[0].AsQueryable().Where(x => x.Age > age).OrderBy(x => x.Id).Expression;

            ShouldBeEqual(PartialEvaluator.Evaluate(Build()).Expression, PartialEvaluator.Evaluate(Build()).Expression);

            Comparer.Equals(Expression.Constant(new Member[0].AsQueryable()), Expression.Constant(new Team[0].AsQueryable())).Should().BeFalse();
        }

        [TestMethod]
        public void Projections_are_compared_by_constructor_and_bindings()
        {
            Expression<Func<Member, object>> Anonymous() => x => new { x.Id, x.Name };
            Expression<Func<Member, Team>> Init() => x => new Team { Id = x.Id, Name = x.Name };
            Expression<Func<Member, Team>> otherInit = x => new Team { Id = x.Age, Name = x.Name };

            ShouldBeEqual(Anonymous(), Anonymous());
            ShouldBeEqual(Init(), Init());
            Comparer.Equals(Init(), otherInit).Should().BeFalse();
        }

        [TestMethod]
        public void Conditional_and_coalesce_are_compared()
        {
            Expression<Func<Member, string>> Build() => x => x.Age > 18 ? x.Name ?? "" : "minor";
            Expression<Func<Member, string>> other = x => x.Age > 18 ? x.Name ?? "" : "adult";

            ShouldBeEqual(Build(), Build());
            Comparer.Equals(Build(), other).Should().BeFalse();
        }

        [TestMethod]
        public void Null_handling()
        {
            Expression<Func<Member, bool>> expression = x => x.Age > 30;

            Comparer.Equals(null, null).Should().BeTrue();
            Comparer.Equals(expression, null).Should().BeFalse();
            Comparer.Equals(null, expression).Should().BeFalse();
            Comparer.GetHashCode(null).Should().Be(0);
        }

        private static void ShouldBeEqual(Expression x, Expression y)
        {
            x.Should().NotBeSameAs(y);
            Comparer.Equals(x, y).Should().BeTrue();
            Comparer.GetHashCode(x).Should().Be(Comparer.GetHashCode(y));
        }

        private class Member
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }

        private class Team
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }
    }
}
