using System;
using System.Linq;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    // 放在命名空間內，才會優先於 Chef.DbAccess.SqlServer.Tests 底下整合測試用的 Member。
    using Department = Characterization.Department;
    using Member = Characterization.Member;

    /// <summary>
    ///     <c>IDataAccess&lt;T&gt;.Query()</c> 沿用 3.x 的資料表名稱與 <c>IsDirtyRead</c> 設定。以 <c>ToQueryString()</c> 驗證，不連線。
    ///     對照 3.x 的特徵化快照（Characterization/Snapshots/QueryCharacterizationTest），只差結尾的分號。
    /// </summary>
    [TestClass]
    public class SqlServerDataAccessQueryTest
    {
        private const string UnreachableConnectionString = "Data Source=127.0.0.1,1;Initial Catalog=Snapshot;Connect Timeout=1;";

        [TestMethod]
        public void Query_from_factory()
        {
            IDataAccess<Member> members = SqlServerDataAccessFactory.Instance.Create<Member>(null, UnreachableConnectionString);

            members.Query().Where(x => x.Id == 1).Select(x => new { x.Name }).ToQueryString().Should().StartWith("SELECT [mem_1].[Name]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)\n");
        }

        [TestMethod]
        public void Join_in_same_database_has_no_database_prefix()
        {
            // 伺服器與資料庫不分大小寫比較。
            var members = new SqlServerDataAccess<Member>(null, UnreachableConnectionString);
            var departments = new SqlServerDataAccess<Department>(null, "Server=127.0.0.1,1;Database=snapshot;Connect Timeout=1");

            members.Query()
                .Join(departments.Query(), m => m.DepartmentId, d => d.Id, (m, d) => d.Name)
                .ToQueryString()
                .Should()
                .Be(
                    "SELECT [dep_2].[Name]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[DepartmentId] = [dep_2].[Id]");
        }

        [TestMethod]
        public void Join_in_other_database_on_same_server_adds_database()
        {
            // 對應 3.x 的跨資料庫 Join（Test_QueryOneAsync_with_InnerJoin_Two_Tables_and_Cross_Database_use_QueryObject）。
            var members = new SqlServerDataAccess<Member>(null, UnreachableConnectionString);
            var departments = new SqlServerDataAccess<Department>(null, "Data Source=127.0.0.1,1;Initial Catalog=Organization;Connect Timeout=1;");

            var query = from m in members.Query()
                        join d in departments.Query() on m.DepartmentId equals d.Id into g
                        from d in g.DefaultIfEmpty()
                        select new { m.Name, DepartmentName = d.Name };

            query.ToQueryString()
                .Should()
                .Be(
                    "SELECT [mem_1].[Name], [dep_2].[Name] AS [DepartmentName]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "LEFT JOIN [Organization].[dbo].[tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[DepartmentId] = [dep_2].[Id]");
        }

        [TestMethod]
        public void Join_on_other_server_throws()
        {
            // 3.x 丟出 ArgumentException("Table is not in the same database server.")。
            var members = new SqlServerDataAccess<Member>(null, UnreachableConnectionString);
            var departments = new SqlServerDataAccess<Department>(null, "Data Source=10.0.0.1;Initial Catalog=Snapshot;");

            Action act = () => members.Query().SelectMany(m => departments.Query(), (m, d) => d.Name).ToQueryString();

            act.Should().Throw<InvalidOperationException>().WithMessage("*'Department'*database server '10.0.0.1'*'127.0.0.1,1'*");
        }

        [TestMethod]
        public void Cached_join_sql_depends_on_database()
        {
            // 兩個查詢的形狀相同，只差在被 Join 的資料庫，不能共用快取的 SQL。
            var members = new SqlServerDataAccess<Member>(null, UnreachableConnectionString);
            var otherDatabase = new SqlServerDataAccess<Department>(null, "Data Source=127.0.0.1,1;Initial Catalog=Organization;");
            var sameDatabase = new SqlServerDataAccess<Department>(null, UnreachableConnectionString);

            string Sql(IDataAccess<Department> departments) =>
                members.Query().Join(departments.Query(), m => m.DepartmentId, d => d.Id, (m, d) => d.Name).ToQueryString();

            Sql(otherDatabase).Should().Contain("[Organization].[dbo].[tbl_department]");
            Sql(sameDatabase).Should().Contain("INNER JOIN [tbl_department]");
            Sql(otherDatabase).Should().Contain("[Organization].[dbo].[tbl_department]");
        }

        [TestMethod]
        public void Query_root_source_comes_from_connection_string()
        {
            // 3.x 的 Regex 要求後面有分號，Initial Catalog 放在最後時取不到資料庫。
            SqlServerDataAccess<Member>.GetQueryRootSource(@"Data Source=(LocalDb)\MSSQLLocalDB;Integrated Security=True;Initial Catalog=Member")
                .Should()
                .Be((@"(LocalDb)\MSSQLLocalDB", "Member"));
            SqlServerDataAccess<Member>.GetQueryRootSource("Server=a.b.c.d;Database=AAA;User ID=abc;Password=cba").Should().Be(("a.b.c.d", "AAA"));
            SqlServerDataAccess<Member>.GetQueryRootSource("not a connection string").Should().Be(((string)null, (string)null));
        }

        [TestMethod]
        public void Explicit_table_name_matches_3x()
        {
            // 對應 3.x Query_Explicit_Table_Name。
            var members = new SqlServerDataAccess<Member>("tbl_member_archive", UnreachableConnectionString);

            members.Query()
                .Where(x => x.Id == 1)
                .Select(x => new { x.Name })
                .ToQueryString()
                .Should()
                .Be("SELECT [mem_1].[Name]\nFROM [tbl_member_archive] [mem_1] WITH (NOLOCK)\nWHERE [mem_1].[No] = {=Id_0}\n\n-- Parameters\n-- @Id_0 = 1 (Int32)");
        }

        [TestMethod]
        public void Without_nolock_when_not_dirty_read_matches_3x()
        {
            // 對應 3.x Query_Without_NoLock_When_Not_DirtyRead。
            var members = new SqlServerDataAccess<Member>(null, UnreachableConnectionString) { IsDirtyRead = false };

            members.Query()
                .Where(x => x.Id == 1)
                .Select(x => new { x.Name })
                .ToQueryString()
                .Should()
                .Be("SELECT [mem_1].[Name]\nFROM [tbl_member] [mem_1]\nWHERE [mem_1].[No] = {=Id_0}\n\n-- Parameters\n-- @Id_0 = 1 (Int32)");
        }

        [TestMethod]
        public void Different_table_names_do_not_share_cached_sql()
        {
            // 快取鍵包含查詢根的資料表名稱，形狀相同但資料表不同的查詢不能共用 SQL。
            string Sql(string tableName) =>
                new SqlServerDataAccess<Member>(tableName, UnreachableConnectionString).Query().Where(x => x.Age > 18).ToQueryString();

            Sql("tbl_member_archive").Should().Contain("FROM [tbl_member_archive] ");
            Sql(null).Should().Contain("FROM [tbl_member] ");
            Sql("tbl_member_archive").Should().Contain("FROM [tbl_member_archive] ");
        }
    }
}
