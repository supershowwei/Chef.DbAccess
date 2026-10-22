using System;
using System.Linq;
using System.Linq.Expressions;
using Chef.DbAccess.Querying;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    [TestClass]
    public class QueryCacheKeyTest
    {
        [TestMethod]
        public void Same_shape_with_different_closure_values_gives_equal_keys()
        {
            var first = Key(Build("Johnny", 30));
            var second = Key(Build("Mary", 40));

            first.Should().Be(second);
            first.GetHashCode().Should().Be(second.GetHashCode());
        }

        [TestMethod]
        public void Null_parameter_value_gives_different_key()
        {
            var notNull = Key(Build("Johnny", 30));
            var nameIsNull = Key(Build(null, 30));

            nameIsNull.Should().NotBe(notNull);
            nameIsNull.Should().Be(Key(Build(null, 40)));
        }

        [TestMethod]
        public void String_value_needing_like_escape_gives_different_key()
        {
            var plain = Key(Build("Johnny", 30));
            var withWildcard = Key(Build("50%", 30));

            // x.Name.Contains(name) 只在值需要跳脫時加上 ESCAPE，SQL 不同。
            withWildcard.Should().NotBe(plain);
            withWildcard.Should().Be(Key(Build("a_b", 40)));
        }

        [TestMethod]
        public void Collection_size_and_null_element_give_different_keys()
        {
            Expression<Func<Member, bool>> BuildAges(int[] ages) => x => ages.Contains(x.Age);
            Expression<Func<Member, bool>> BuildNames(string[] names) => x => names.Contains(x.Name);

            // ids.Contains(x.Id) 輸出 IN，每個元素一個參數；元素有 null 時另外加上 IS NULL。
            Key(BuildAges(new[] { 1, 2 })).Should().Be(Key(BuildAges(new[] { 3, 4 })));
            Key(BuildAges(new[] { 1, 2 })).Should().NotBe(Key(BuildAges(new[] { 1, 2, 3 })));
            Key(BuildNames(new[] { "a", "b" })).Should().NotBe(Key(BuildNames(new[] { "a", null })));
        }

        [TestMethod]
        public void Different_shape_gives_different_key()
        {
            var name = "Johnny";
            Expression<Func<Member, bool>> other = x => x.Name == name || x.Age > 30;

            Key(Build(name, 30)).Should().NotBe(Key(other));
        }

        [TestMethod]
        public void Constructor_null_query_throws()
        {
            Action act = () => new QueryCacheKey(null);

            act.Should().Throw<ArgumentNullException>();
        }

        private static Expression<Func<Member, bool>> Build(string name, int age)
        {
            return x => x.Name == name && x.Age > age;
        }

        private static QueryCacheKey Key(Expression expression)
        {
            return new QueryCacheKey(PartialEvaluator.Evaluate(expression));
        }

        private class Member
        {
            public string Name { get; set; }

            public int Age { get; set; }
        }
    }
}
