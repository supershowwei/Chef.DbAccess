using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlServer.Querying;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    /// <summary>
    ///     把翻譯器產生的 Shaper 編譯後，從 <see cref="DataTableReader" /> 讀出結果；DataTable 的欄位順序對應 SELECT 清單。
    /// </summary>
    [TestClass]
    public class ShaperCompilerTest
    {
        private static IQueryable<Member> Members => new Member[0].AsQueryable();

        [TestMethod]
        public void Entity_is_materialized_and_null_becomes_default()
        {
            var table = Table(
                ("Id", typeof(int)),
                ("Name", typeof(string)),
                ("Age", typeof(int)),
                ("Level", typeof(int)),
                ("Status", typeof(int)),
                ("IsActive", typeof(bool)));

            table.Rows.Add(1, "Johnny", 20, DBNull.Value, 1, true);
            table.Rows.Add(2, DBNull.Value, DBNull.Value, 5, 2, false);

            // 與 Dapper 相同：不可為 null 的 Age 遇到 NULL 時給 0；int 欄位轉成列舉。
            Shape(Members, table)
                .Should()
                .BeEquivalentTo(
                    new Member { Id = 1, Name = "Johnny", Age = 20, Level = null, Status = MemberStatus.Active, IsActive = true },
                    new Member { Id = 2, Name = null, Age = 0, Level = 5, Status = MemberStatus.Resigned, IsActive = false });
        }

        [TestMethod]
        public void Anonymous_type()
        {
            var table = Table(("Id", typeof(int)), ("N", typeof(string)));
            table.Rows.Add(1, "Johnny");

            Shape(Members.Select(x => new { x.Id, N = x.Name }), table).Should().BeEquivalentTo(new[] { new { Id = 1, N = "Johnny" } });
        }

        [TestMethod]
        public void Dto_with_member_init()
        {
            var table = Table(("Key", typeof(int)), ("Title", typeof(string)));
            table.Rows.Add(1, "Johnny");

            Shape(Members.Select(x => new MemberDto { Key = x.Id, Title = x.Name }), table).Should().BeEquivalentTo(new MemberDto { Key = 1, Title = "Johnny" });
        }

        [TestMethod]
        public void Case_when_int_is_converted_to_bool()
        {
            // SQL Server 的 CASE WHEN ... THEN 1 ELSE 0 END 回傳 int。
            var table = Table(("Adult", typeof(int)));
            table.Rows.Add(1);
            table.Rows.Add(0);

            Shape(Members.Select(x => new { Adult = x.Age >= 18 }), table).Select(r => r.Adult).Should().Equal(true, false);
        }

        [TestMethod]
        public void Column_type_different_from_property_is_converted()
        {
            // 例如 tinyint 欄位對應到 int 屬性。
            var table = Table(("Age", typeof(byte)));
            table.Rows.Add((byte)20);

            Shape(Members.Select(x => x.Age), table).Should().Equal(20);
        }

        [TestMethod]
        public void Client_values_come_from_parameter_values()
        {
            var tag = "vip";
            var table = Table(("Id", typeof(int)));
            table.Rows.Add(1);

            Shape(Members.Select(x => new { x.Id, Tag = tag, Fixed = 1 }), table).Should().BeEquivalentTo(new[] { new { Id = 1, Tag = "vip", Fixed = 1 } });
        }

        [TestMethod]
        public void Scalar_and_nullable_scalar()
        {
            var table = Table(("Value", typeof(int)));
            table.Rows.Add(3);
            table.Rows.Add(DBNull.Value);

            ReadAll(ShaperCompiler.Compile<int?>(new ProjectionBindingExpression(0, typeof(int?))), table).Should().Equal(3, null);
        }

        [TestMethod]
        public void Unconvertible_value_throws_with_column_value()
        {
            var table = Table(("Value", typeof(string)));
            table.Rows.Add("abc");

            Action act = () => ReadAll(ShaperCompiler.Compile<int>(new ProjectionBindingExpression(0, typeof(int))), table);

            act.Should().Throw<InvalidCastException>().WithMessage("*'abc'*System.Int32*");
        }

        private static List<T> Shape<T>(IQueryable<T> query, DataTable table)
        {
            var parameterized = PartialEvaluator.Evaluate(query.Expression);
            var translated = new QueryableMethodTranslator(
                    new SqlTranslationContext(parameterized.ParameterValues),
                    SqlServerTranslators.Members,
                    SqlServerTranslators.MethodCalls)
                .Translate(parameterized.Expression);

            return ReadAll(ShaperCompiler.Compile<T>(translated.Shaper), table, parameterized.ParameterValues);
        }

        private static List<T> ReadAll<T>(
            Func<System.Data.Common.DbDataReader, IReadOnlyDictionary<string, object>, T> shaper,
            DataTable table,
            IReadOnlyDictionary<string, object> parameterValues = null)
        {
            var results = new List<T>();

            using (var reader = table.CreateDataReader())
            {
                while (reader.Read()) results.Add(shaper(reader, parameterValues ?? new Dictionary<string, object>()));
            }

            return results;
        }

        private static DataTable Table(params (string Name, Type Type)[] columns)
        {
            var table = new DataTable();

            foreach (var (name, type) in columns) table.Columns.Add(name, type);

            return table;
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

            public string Name { get; set; }

            public int Age { get; set; }

            public int? Level { get; set; }

            public MemberStatus Status { get; set; }

            public bool IsActive { get; set; }
        }

        private class MemberDto
        {
            public int Key { get; set; }

            public string Title { get; set; }
        }
    }
}
