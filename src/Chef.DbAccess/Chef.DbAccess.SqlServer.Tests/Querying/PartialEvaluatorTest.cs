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
    public class PartialEvaluatorTest
    {
        [TestMethod]
        public void Closure_variable_becomes_named_parameter()
        {
            var age = 30;

            var result = Evaluate(x => x.Age > age);

            Right(result).Should().BeAssignableTo<ParameterExpression>().Which.Name.Should().Be("__age_0");
            result.ParameterValues.Should().BeEquivalentTo(new Dictionary<string, object> { ["__age_0"] = 30 });
        }

        [TestMethod]
        public void Same_shape_with_different_closure_values_gives_same_expression()
        {
            Expression<Func<Member, bool>> Build(int age) => x => x.Age > age;

            var first = PartialEvaluator.Evaluate(Build(30));
            var second = PartialEvaluator.Evaluate(Build(40));

            first.Expression.ToString().Should().Be(second.Expression.ToString());
            first.ParameterValues["__age_0"].Should().Be(30);
            second.ParameterValues["__age_0"].Should().Be(40);
        }

        [TestMethod]
        public void Computed_value_without_member_name_uses_generic_parameter_name()
        {
            var age = 30;

            var result = Evaluate(x => x.Age > age + 1);

            Right(result).Should().BeAssignableTo<ParameterExpression>().Which.Name.Should().Be("__p_0");
            result.ParameterValues["__p_0"].Should().Be(31);
        }

        [TestMethod]
        public void Static_member_and_method_call_are_evaluated()
        {
            var expected = DateTime.Today.AddDays(-7);

            var result = Evaluate(x => x.CreatedAt > DateTime.Today.AddDays(-7));

            Right(result).Should().BeAssignableTo<ParameterExpression>().Which.Name.Should().Be("__AddDays_0");
            result.ParameterValues["__AddDays_0"].Should().Be(expected);
        }

        [TestMethod]
        public void Literal_constant_stays_unchanged()
        {
            Expression<Func<Member, bool>> expression = x => x.Age > 30 && x.Name == "Johnny";

            var result = PartialEvaluator.Evaluate(expression);

            result.Expression.Should().BeSameAs(expression);
            result.ParameterValues.Should().BeEmpty();
        }

        [TestMethod]
        public void Constant_only_subtree_folds_into_constant()
        {
            var result = Evaluate(x => x.CreatedAt > new DateTime(2020, 1, 1));

            Right(result).Should().BeAssignableTo<ConstantExpression>().Which.Value.Should().Be(new DateTime(2020, 1, 1));
            result.ParameterValues.Should().BeEmpty();
        }

        [TestMethod]
        public void Constant_converted_to_nullable_folds_into_constant()
        {
            var result = Evaluate(x => x.Level == 3);

            var right = Right(result).Should().BeAssignableTo<ConstantExpression>().Which;
            right.Type.Should().Be(typeof(int?));
            right.Value.Should().Be(3);
            result.ParameterValues.Should().BeEmpty();
        }

        [TestMethod]
        public void Subtree_depending_on_lambda_parameter_is_not_evaluated()
        {
            Expression<Func<Member, bool>> expression = x => x.Name.Length > 3 && x.Name.StartsWith("J");

            var result = PartialEvaluator.Evaluate(expression);

            result.Expression.Should().BeSameAs(expression);
        }

        [TestMethod]
        public void Nested_lambda_over_closure_collection_is_evaluated_as_a_whole()
        {
            var ids = new List<int> { 1, 2, 3 };

            var result = Evaluate(x => x.Age > ids.Where(i => i > 1).Count());

            Right(result).Should().BeAssignableTo<ParameterExpression>().Which.Name.Should().Be("__Count_0");
            result.ParameterValues["__Count_0"].Should().Be(2);
        }

        [TestMethod]
        public void Nested_lambda_referencing_outer_parameter_is_not_evaluated()
        {
            var ids = new List<int> { 1, 2, 3 };

            var result = Evaluate(x => ids.Any(i => i == x.Id));

            var any = ((LambdaExpression)result.Expression).Body.Should().BeAssignableTo<MethodCallExpression>().Which;
            any.Method.Name.Should().Be(nameof(Enumerable.Any));
            any.Arguments[0].Should().BeAssignableTo<ParameterExpression>().Which.Name.Should().Be("__ids_0");
            any.Arguments[1].Should().BeAssignableTo<LambdaExpression>();
            result.ParameterValues["__ids_0"].Should().BeSameAs(ids);
        }

        [TestMethod]
        public void Query_root_and_queryable_operators_are_not_evaluated()
        {
            var age = 30;
            var source = new Member[0].AsQueryable();

            var result = PartialEvaluator.Evaluate(source.Where(x => x.Age > age).OrderBy(x => x.Id).Expression);

            var orderBy = result.Expression.Should().BeAssignableTo<MethodCallExpression>().Which;
            var where = orderBy.Arguments[0].Should().BeAssignableTo<MethodCallExpression>().Which;
            orderBy.Method.Name.Should().Be(nameof(Queryable.OrderBy));
            where.Method.Name.Should().Be(nameof(Queryable.Where));
            where.Arguments[0].Should().BeSameAs(source.Expression);
            result.ParameterValues.Should().BeEquivalentTo(new Dictionary<string, object> { ["__age_0"] = 30 });
        }

        [TestMethod]
        public void Closure_queryable_is_inlined_as_its_expression()
        {
            var source = new Member[0].AsQueryable();
            var others = new Member[0].AsQueryable();

            var result = PartialEvaluator.Evaluate(source.Where(x => others.Any(o => o.Id == x.Id)).Expression);

            var where = (MethodCallExpression)result.Expression;
            var predicate = (LambdaExpression)((UnaryExpression)where.Arguments[1]).Operand;
            var any = predicate.Body.Should().BeAssignableTo<MethodCallExpression>().Which;
            any.Method.DeclaringType.Should().Be(typeof(Queryable));
            any.Arguments[0].Should().BeSameAs(others.Expression);
            result.ParameterValues.Should().BeEmpty();
        }

        [TestMethod]
        public void Null_closure_instance_throws_NullReferenceException()
        {
            Member other = null;

            Action act = () => Evaluate(x => x.Id == other.Id);

            act.Should().Throw<NullReferenceException>();
        }

        [TestMethod]
        public void Exception_from_property_getter_is_not_wrapped()
        {
            var thrower = new Thrower();

            Action act = () => Evaluate(x => x.Age == thrower.Value);

            act.Should().Throw<InvalidOperationException>().WithMessage("boom");
        }

        [TestMethod]
        public void Evaluate_null_expression_throws()
        {
            Action act = () => PartialEvaluator.Evaluate(null);

            act.Should().Throw<ArgumentNullException>();
        }

        private static ParameterizedQuery Evaluate(Expression<Func<Member, bool>> predicate)
        {
            return PartialEvaluator.Evaluate(predicate);
        }

        private static Expression Right(ParameterizedQuery result)
        {
            return ((BinaryExpression)((LambdaExpression)result.Expression).Body).Right;
        }

        private class Member
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }

            public int? Level { get; set; }

            public DateTime CreatedAt { get; set; }
        }

        private class Thrower
        {
            public int Value => throw new InvalidOperationException("boom");
        }
    }
}
