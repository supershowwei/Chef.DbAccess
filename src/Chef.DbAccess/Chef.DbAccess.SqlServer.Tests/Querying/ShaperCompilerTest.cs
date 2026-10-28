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

        [TestMethod]
        public void Left_join_entity_without_row_is_null()
        {
            var query = from m in Members
                        join d in new Department[0].AsQueryable() on m.Id equals d.ManagerId into g
                        from d in g.DefaultIfEmpty()
                        select new { m.Name, Department = d };

            // SELECT [mem_1].[Name], [dep_2].[Id], [dep_2].[Name] AS [Name1], [dep_2].[ManagerId]；以 ON 條件中的 [ManagerId] 判斷右邊有沒有資料。
            var table = Table(("Name", typeof(string)), ("Id", typeof(int)), ("Name1", typeof(string)), ("ManagerId", typeof(int)));
            table.Rows.Add("Johnny", 1, "業務", 1);
            table.Rows.Add("Amy", DBNull.Value, DBNull.Value, DBNull.Value);

            var results = Shape(query, table);

            results[0].Department.Should().BeEquivalentTo(new Department { Id = 1, Name = "業務", ManagerId = 1 });
            results[1].Name.Should().Be("Amy");
            results[1].Department.Should().BeNull();
        }

        [TestMethod]
        public void Include_fills_navigation_or_null()
        {
            // SELECT [sta_1].[Id], [sta_1].[Name], [sta_1].[DepartmentId], [dep_2].[Id] AS [Id1], [dep_2].[Name] AS [Name1], [dep_2].[ManagerId]
            var table = Table(
                ("Id", typeof(int)),
                ("Name", typeof(string)),
                ("DepartmentId", typeof(int)),
                ("Id1", typeof(int)),
                ("Name1", typeof(string)),
                ("ManagerId", typeof(int)));

            table.Rows.Add(1, "Johnny", 3, 3, "業務", 1);
            table.Rows.Add(2, "Amy", 9, DBNull.Value, DBNull.Value, DBNull.Value);

            var results = Shape(new Staff[0].AsQueryable().Include(s => s.Department), table);

            results[0].Should().BeEquivalentTo(new Staff { Id = 1, Name = "Johnny", DepartmentId = 3, Department = new Department { Id = 3, Name = "業務", ManagerId = 1 } });
            results[1].Name.Should().Be("Amy");
            results[1].Department.Should().BeNull();
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

        [Table("tbl_department")]
        private class Department
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int ManagerId { get; set; }
        }

        [Table("tbl_staff")]
        private class Staff
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int DepartmentId { get; set; }

            [ForeignKey(nameof(DepartmentId))]
            public Department Department { get; set; }
        }
    }
}
