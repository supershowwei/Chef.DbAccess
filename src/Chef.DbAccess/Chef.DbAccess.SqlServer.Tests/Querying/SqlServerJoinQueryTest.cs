using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    /// <summary>
    ///     在 LocalDB 實際執行 Phase 3 的 Join、導覽屬性與 Include，經過 <c>SqlServerDataAccess.Query()</c> 的完整流程。
    ///     <para>
    ///         測試資料表建在 tempdb；跨資料庫的測試另外建立 <see cref="OtherDatabase" />，測試後刪除。
    ///         會員 4 位：Johnny、Mary 在 Sales，Bob 在 HR，Amy 沒有部門；Sales 的主管是 Johnny、HR 是 Bob、RD 沒有主管也沒有會員。
    ///     </para>
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    public class SqlServerJoinQueryTest
    {
        private const string Server = @"(LocalDb)\MSSQLLocalDB";
        private const string OtherDatabase = "ChefDbAccessJoinTest";
        private const string ConnectionString = "Data Source=" + Server + ";Initial Catalog=tempdb;Integrated Security=True";
        private const string OtherConnectionString = "Data Source=" + Server + ";Initial Catalog=" + OtherDatabase + ";Integrated Security=True";

        private static IQueryable<Member> Members => new SqlServerDataAccess<Member>(null, ConnectionString).Query();

        private static IQueryable<Department> Departments => new SqlServerDataAccess<Department>(null, ConnectionString).Query();

        [ClassInitialize]
        public static void CreateTables(TestContext context)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Execute(
                    @"
IF OBJECT_ID('dbo.chef_join_member') IS NOT NULL DROP TABLE dbo.chef_join_member;
IF OBJECT_ID('dbo.chef_join_department') IS NOT NULL DROP TABLE dbo.chef_join_department;

CREATE TABLE dbo.chef_join_department (Id int NOT NULL PRIMARY KEY, Name varchar(20) NOT NULL, ManagerId int NULL);
CREATE TABLE dbo.chef_join_member (No int NOT NULL PRIMARY KEY, Name varchar(20) NOT NULL, DepartmentId int NULL, ManagerId int NULL);

INSERT INTO dbo.chef_join_department VALUES (1, 'Sales', 1), (2, 'HR', 3), (3, 'RD', NULL);
INSERT INTO dbo.chef_join_member VALUES (1, 'Johnny', 1, NULL), (2, 'Mary', 1, 1), (3, 'Bob', 2, 1), (4, 'Amy', NULL, 3);

IF DB_ID('" + OtherDatabase + @"') IS NOT NULL
BEGIN
    ALTER DATABASE " + OtherDatabase + @" SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE " + OtherDatabase + @";
END

CREATE DATABASE " + OtherDatabase + @";");
            }

            using (var connection = new SqlConnection(OtherConnectionString))
            {
                connection.Execute(
                    @"
CREATE TABLE dbo.chef_join_department (Id int NOT NULL PRIMARY KEY, Name varchar(20) NOT NULL, ManagerId int NULL);
INSERT INTO dbo.chef_join_department VALUES (1, 'OtherSales', NULL), (2, 'OtherHR', NULL);");
            }
        }

        [ClassCleanup]
        public static void DropTables()
        {
            // 連線集區還連著另一個資料庫時不能刪除。
            SqlConnection.ClearAllPools();

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Execute(
                    @"
IF OBJECT_ID('dbo.chef_join_member') IS NOT NULL DROP TABLE dbo.chef_join_member;
IF OBJECT_ID('dbo.chef_join_department') IS NOT NULL DROP TABLE dbo.chef_join_department;

IF DB_ID('" + OtherDatabase + @"') IS NOT NULL
BEGIN
    ALTER DATABASE " + OtherDatabase + @" SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE " + OtherDatabase + @";
END");
            }
        }

        [TestMethod]
        public void Inner_join()
        {
            var query = from m in Members
                        join d in Departments on m.DepartmentId equals (int?)d.Id
                        orderby m.Id
                        select new { m.Name, DepartmentName = d.Name };

            query.ToList()
                .Should()
                .BeEquivalentTo(
                    new[] { new { Name = "Johnny", DepartmentName = "Sales" }, new { Name = "Mary", DepartmentName = "Sales" }, new { Name = "Bob", DepartmentName = "HR" } },
                    o => o.WithStrictOrdering());
        }

        [TestMethod]
        public void Left_join_gives_null_entity_without_match()
        {
            var query = from m in Members
                        join d in Departments on m.DepartmentId equals (int?)d.Id into g
                        from d in g.DefaultIfEmpty()
                        orderby m.Id
                        select new { m.Name, Department = d };

            var results = query.ToList();

            results.Select(r => r.Department?.Name).Should().Equal("Sales", "Sales", "HR", null);
            results[3].Name.Should().Be("Amy");
        }

        [TestMethod]
        public void Cross_join_with_where()
        {
            (from m in Members from d in Departments select m.Id).Count().Should().Be(12);

            var managers = from m in Members
                           from d in Departments
                           where d.ManagerId == m.Id
                           orderby d.Id
                           select new { Department = d.Name, Manager = m.Name };

            managers.ToList()
                .Should()
                .BeEquivalentTo(new[] { new { Department = "Sales", Manager = "Johnny" }, new { Department = "HR", Manager = "Bob" } }, o => o.WithStrictOrdering());
        }

        [TestMethod]
        public void Navigation_properties_in_where_and_select()
        {
            var query = Members.Where(m => m.Department.Name == "Sales")
                .OrderBy(m => m.Id)
                .Select(m => new { m.Name, DepartmentManager = m.Department.Manager.Name, Manager = m.Manager.Name });

            query.ToList()
                .Should()
                .BeEquivalentTo(
                    new[] { new { Name = "Johnny", DepartmentManager = "Johnny", Manager = (string)null }, new { Name = "Mary", DepartmentManager = "Johnny", Manager = "Johnny" } },
                    o => o.WithStrictOrdering());
        }

        [TestMethod]
        public void Include_reference_navigations()
        {
            var members = Members.Include(m => m.Department.Manager).Include(m => m.Manager).OrderBy(m => m.Id).ToList();

            members.Select(m => m.Department?.Name).Should().Equal("Sales", "Sales", "HR", null);
            members.Select(m => m.Department?.Manager?.Name).Should().Equal("Johnny", "Johnny", "Bob", null);
            members.Select(m => m.Manager?.Name).Should().Equal(null, "Johnny", "Johnny", "Bob");
        }

        [TestMethod]
        public void Include_with_condition_and_columns()
        {
            var members = Members.Include(m => m.Department, d => d.Name).OrderBy(m => m.Id).ToList();

            // 只填入 Name；Id 只用來判斷有沒有對應的部門。
            members[0].Department.Should().BeEquivalentTo(new Department { Id = 0, Name = "Sales" });
            members[3].Department.Should().BeNull();

            var managers = Departments.Include(d => d.ManagerMember, (d, m) => d.ManagerId == m.Id).OrderBy(d => d.Id).ToList();

            managers.Select(d => d.ManagerMember?.Name).Should().Equal("Johnny", "Bob", null);
        }

        [TestMethod]
        public async Task Include_collection_merges_rows()
        {
            var query = Departments.Include(d => d.Members, (d, m) => d.Id == m.DepartmentId).OrderBy(d => d.Id);

            foreach (var departments in new[] { query.ToList(), await query.ToListAsync() })
            {
                departments.Select(d => d.Name).Should().Equal("Sales", "HR", "RD");
                departments[0].Members.Select(m => m.Name).Should().BeEquivalentTo("Johnny", "Mary");
                departments[1].Members.Select(m => m.Name).Should().Equal("Bob");
                departments[2].Members.Should().BeEmpty();
            }

            // 一對多的 Join 不影響筆數。
            query.Count().Should().Be(3);
        }

        [TestMethod]
        public void Include_collection_with_columns()
        {
            var sales = Departments.Include(d => d.Members, (d, m) => d.Id == m.DepartmentId, m => m.Name).Where(d => d.Id == 1).ToList().Single();

            // 鍵（No）自動填入，DepartmentId 沒有指定所以是 null。
            sales.Members.Select(m => (m.Id, m.Name, m.DepartmentId)).Should().BeEquivalentTo(new[] { (1, "Johnny", (int?)null), (2, "Mary", (int?)null) });
        }

        [TestMethod]
        public void Join_table_in_other_database()
        {
            var otherDepartments = new SqlServerDataAccess<Department>(null, OtherConnectionString).Query();

            var query = from m in Members
                        join d in otherDepartments on m.DepartmentId equals (int?)d.Id
                        orderby m.Id
                        select new { m.Name, DepartmentName = d.Name };

            query.ToQueryString().Should().Contain("INNER JOIN [" + OtherDatabase + "].[dbo].[chef_join_department]");
            query.ToList().Select(r => r.DepartmentName).Should().Equal("OtherSales", "OtherSales", "OtherHR");
        }

        [Table("chef_join_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            public string Name { get; set; }

            public int? DepartmentId { get; set; }

            [ForeignKey(nameof(DepartmentId))]
            public Department Department { get; set; }

            public int? ManagerId { get; set; }

            [ForeignKey(nameof(ManagerId))]
            public Member Manager { get; set; }
        }

        [Table("chef_join_department")]
        private class Department
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int? ManagerId { get; set; }

            [ForeignKey(nameof(ManagerId))]
            public Member Manager { get; set; }

            // 沒有 [ForeignKey]，以條件 Include。
            public Member ManagerMember { get; set; }

            public List<Member> Members { get; set; }
        }
    }
}
