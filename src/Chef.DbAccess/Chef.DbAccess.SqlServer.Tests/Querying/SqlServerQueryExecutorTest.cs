using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Chef.DbAccess.Querying;
using Chef.DbAccess.SqlServer.Querying;
using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    /// <summary>
    ///     在 LocalDB 實際執行新的查詢管線（LINQ → SQL → Dapper → Shaper）。
    ///     測試資料表建在 tempdb，測試前建立、測試後刪除，不需要事先附加測試資料庫。
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    public class SqlServerQueryExecutorTest
    {
        private const string ConnectionString = @"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=tempdb;Integrated Security=True";

        private static readonly QueryCompiler Compiler = new QueryCompiler(SqlServerDialect.Instance);

        private static IQueryable<Member> Members => new ChefQueryable<Member>(new ChefQueryProvider(Compiler, new SqlServerQueryExecutor(ConnectionString)));

        [ClassInitialize]
        public static void CreateTable(TestContext context)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Execute(
                    @"
IF OBJECT_ID('dbo.chef_query_member') IS NOT NULL DROP TABLE dbo.chef_query_member;

CREATE TABLE dbo.chef_query_member (
    No int NOT NULL PRIMARY KEY,
    Name varchar(20) NULL,
    Age int NOT NULL,
    Level int NULL,
    Status tinyint NOT NULL,
    IsActive bit NOT NULL,
    CreatedAt datetime2(3) NOT NULL);

INSERT INTO dbo.chef_query_member VALUES
    (1, 'Johnny', 20, NULL, 1, 1, '2026-01-01T08:00:00'),
    (2, 'Mary', 17, 5, 2, 0, '2026-01-02T08:00:00'),
    (3, '50% off', 30, NULL, 1, 1, '2026-02-01T08:00:00'),
    (4, 'a_b', 40, 3, 1, 0, '2026-03-01T08:00:00'),
    (5, NULL, 25, NULL, 2, 1, '2025-12-31T08:00:00');");
            }
        }

        [ClassCleanup]
        public static void DropTable()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Execute("IF OBJECT_ID('dbo.chef_query_member') IS NOT NULL DROP TABLE dbo.chef_query_member;");
            }
        }

        [TestMethod]
        public void Entities_are_materialized()
        {
            var members = Members.OrderBy(x => x.Id).ToList();

            members.Should().HaveCount(5);

            // tinyint 欄位轉成列舉、NULL 的 Level 是 null。
            members[0]
                .Should()
                .BeEquivalentTo(
                    new Member
                    {
                        Id = 1,
                        Name = "Johnny",
                        Age = 20,
                        Level = null,
                        Status = MemberStatus.Active,
                        IsActive = true,
                        CreatedAt = new DateTime(2026, 1, 1, 8, 0, 0)
                    });
            members[4].Name.Should().BeNull();
        }

        [TestMethod]
        public void Varchar_and_literal_parameters()
        {
            Members.Where(x => x.Name == "Mary" && x.Age > 10).Select(x => x.Id).ToList().Should().Equal(2);
        }

        [TestMethod]
        public void Like_wildcards_in_value_are_escaped()
        {
            // 3.x 不跳脫，Contains("_") 會比對到所有有名字的資料。
            Members.Where(x => x.Name.Contains("%")).Select(x => x.Id).ToList().Should().Equal(3);
            Members.Where(x => x.Name.Contains("_")).Select(x => x.Id).ToList().Should().Equal(4);
            Members.Where(x => x.Name.StartsWith("J")).Select(x => x.Id).ToList().Should().Equal(1);
        }

        [TestMethod]
        public void Projection_with_case_when()
        {
            Members.Where(x => x.Id <= 2)
                .OrderBy(x => x.Id)
                .Select(x => new { x.Id, Adult = x.Age >= 18 })
                .ToList()
                .Should()
                .BeEquivalentTo(new[] { new { Id = 1, Adult = true }, new { Id = 2, Adult = false } }, o => o.WithStrictOrdering());
        }

        [TestMethod]
        public void Skip_and_Take()
        {
            Members.OrderBy(x => x.Id).Skip(1).Take(2).Select(x => x.Id).ToList().Should().Equal(2, 3);
        }

        [TestMethod]
        public void Result_operators()
        {
            Members.OrderBy(x => x.Id).First(x => x.Age > 25).Id.Should().Be(3);
            Members.FirstOrDefault(x => x.Id == 99).Should().BeNull();
            Members.Single(x => x.Id == 2).Name.Should().Be("Mary");
            Members.Count(x => x.IsActive).Should().Be(3);
            Members.LongCount().Should().Be(5);
            Members.Any(x => x.Age > 35).Should().BeTrue();
            Members.All(x => x.Age > 10).Should().BeTrue();
            Members.All(x => x.Age > 18).Should().BeFalse();
        }

        [TestMethod]
        public void Collection_Contains_with_null_element()
        {
            var names = new[] { "Mary", null };

            Members.Where(x => names.Contains(x.Name)).OrderBy(x => x.Id).Select(x => x.Id).ToList().Should().Equal(2, 5);
        }

        [TestMethod]
        public void Cached_query_takes_new_values()
        {
            int CountOlderThan(int age) => Members.Count(x => x.Age > age);

            CountOlderThan(18).Should().Be(4);
            CountOlderThan(30).Should().Be(1);
        }

        [TestMethod]
        public void DateTime_and_string_functions()
        {
            // AddDays(1.5) 的小數部分換算成毫秒，比較的值以 datetime2 送出。
            Members.Where(x => x.CreatedAt.Year == 2026 && x.CreatedAt.AddDays(1.5) > new DateTime(2026, 3, 2, 12, 0, 0))
                .Select(x => x.Id)
                .ToList()
                .Should()
                .Equal(4);

            Members.Where(x => x.Name.Length == 4).Select(x => x.Id).ToList().Should().Equal(2);
            Members.Where(x => x.Name.ToUpper() == "JOHNNY").Select(x => x.Id).ToList().Should().Equal(1);
        }

        private enum MemberStatus
        {
            Active = 1,
            Resigned = 2
        }

        [Table("chef_query_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            [Column(TypeName = "varchar")]
            [StringLength(20)]
            public string Name { get; set; }

            public int Age { get; set; }

            public int? Level { get; set; }

            public MemberStatus Status { get; set; }

            public bool IsActive { get; set; }

            [Column(TypeName = "datetime2")]
            public DateTime CreatedAt { get; set; }
        }
    }
}
