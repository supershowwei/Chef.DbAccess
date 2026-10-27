using System.Linq;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Querying
{
    // 放在命名空間內，才會優先於 Chef.DbAccess.SqlServer.Tests 底下整合測試用的 Member。
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
