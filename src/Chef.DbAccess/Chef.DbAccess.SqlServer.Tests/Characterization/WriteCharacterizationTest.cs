using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    ///     記錄 3.x 寫入 API（Insert / Update / Upsert / Delete / Bulk）產生的 SQL，作為 4.0 改寫時的回歸基準。
    ///     Bulk 系列只記錄主要語句，TVP 的 CREATE TYPE 與資料列不在快照內。
    /// </summary>
    [TestClass]
    public class WriteCharacterizationTest
    {
        private readonly SqlServerDataAccess<Member> members = SqlSnapshot.CreateDataAccess<Member>();

        private static List<Member> Values =>
            new List<Member>
            {
                new Member { Id = 324, Name = "Cruzer", Phone = "0000-000123", Age = 12, DepartmentId = 1 },
                new Member { Id = 537, Name = "Slice", Phone = "0000-000456", Age = 21, DepartmentId = 2 }
            };

        [TestMethod]
        public Task Insert_Entity()
        {
            return SqlSnapshot.MatchAsync(() => this.members.InsertAsync(new Member { Id = 1, Name = "Johnny", DepartmentId = 1 }));
        }

        [TestMethod]
        public Task Insert_Entity_With_Nonexistence()
        {
            return SqlSnapshot.MatchAsync(() => this.members.InsertAsync(new Member { Id = 1, Name = "Johnny", DepartmentId = 1 }, x => x.Id == 1));
        }

        [TestMethod]
        public Task Insert_Setter()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Set(() => new Member { Id = 1, Name = "Johnny", Phone = "0912345678", Age = 30 }).InsertAsync());
        }

        [TestMethod]
        public Task Insert_Setter_With_Output()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Set(() => new Member { Name = "Johnny", Age = 30 }).InsertAsync(x => new { x.Id }));
        }

        [TestMethod]
        public Task Insert_Many()
        {
            return SqlSnapshot.MatchAsync(() => this.members.InsertAsync(Values));
        }

        [TestMethod]
        public Task BulkInsert_Required_Columns()
        {
            return SqlSnapshot.MatchAsync(() => this.members.BulkInsertAsync(Values));
        }

        [TestMethod]
        public Task BulkInsert_Setter_Template_With_Output()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.Set(() => new Member { Id = default, Name = default, Phone = default, Age = default }).BulkInsertAsync(Values, x => new { x.Id }));
        }

        [TestMethod]
        public Task Update_Setter()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Set(() => new Member { Phone = "02-87871234" }).Where(x => x.Id == 1).UpdateAsync());
        }

        [TestMethod]
        public Task Update_Chained_Set()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Set(x => x.Age, 20).Set(x => x.Phone, "02-87871234").Where(x => x.Id == 1).UpdateAsync());
        }

        [TestMethod]
        public Task Update_InnerJoin()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                          .Set(x => x.Age, 20)
                          .Where((x, y) => y.Name.Contains("業務"))
                          .UpdateAsync());
        }

        [TestMethod]
        public Task Update_Many_With_Templates()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Set(() => new Member { Age = default, Phone = default }).Where(x => x.Id == default).UpdateAsync(Values));
        }

        [TestMethod]
        public Task BulkUpdate()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id == default).Set(() => new Member { Name = default, Phone = default }).BulkUpdateAsync(Values));
        }

        [TestMethod]
        public Task Upsert()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id == 1).Set(() => new Member { Name = "Johnny", Phone = "0912345678" }).UpsertAsync());
        }

        [TestMethod]
        public Task Upsert_With_Output()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id == 1).Set(() => new Member { Name = "Johnny" }).UpsertAsync(x => new { x.Id, x.Name }));
        }

        [TestMethod]
        public Task BulkUpsert_With_Output()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.Where(x => x.Id == default).Set(() => new Member { Name = default, Phone = default }).BulkUpsertAsync(Values, x => new { x.Id }));
        }

        [TestMethod]
        public Task Delete()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age < 18).DeleteAsync());
        }

        [TestMethod]
        public Task Delete_With_Output()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age < 18).DeleteAsync(x => new { x.Id, x.Name }));
        }

        [TestMethod]
        public Task Delete_InnerJoin()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id).Where((x, y) => y.Name == "業務部").DeleteAsync());
        }
    }
}
