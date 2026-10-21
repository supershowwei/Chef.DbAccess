using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.Syntax;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    ///     記錄 3.x 查詢 API 產生的 SQL，作為 4.0 改寫時的回歸基準。
    ///     這裡記錄的是「現有行為」，包含已知的限制與陷阱，不代表期望行為。
    /// </summary>
    [TestClass]
    public class QueryCharacterizationTest
    {
        private readonly SqlServerDataAccess<Member> members = SqlSnapshot.CreateDataAccess<Member>();

        [TestMethod]
        public Task QueryOne_Where_Select()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id == 1).Select(x => new { x.Name, x.Age }).QueryOneAsync());
        }

        [TestMethod]
        public Task Query_Array_Contains()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => new[] { 1, 2, 3 }.Contains(x.Id)).Select(x => new { x.Name, x.Age }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Array_Not_Contains()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => !new[] { 1, 2, 3 }.Contains(x.Id)).Select(x => new { x.Name, x.Age }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Empty_Collection_Contains()
        {
            var ids = new List<int>();

            return SqlSnapshot.MatchAsync(() => this.members.Where(x => ids.Contains(x.Id)).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_StartsWith_OrderBy_Take()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.Where(x => x.Age >= 30 && x.Name.StartsWith("J")).OrderBy(x => x.Age).Take(10).Select(x => new { x.Name, x.Age }).QueryAsync());
        }

        [TestMethod]
        public Task Query_String_Contains_EndsWith_And_Not()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.Where(x => x.Name.Contains("e") && x.Name.EndsWith("y") && !x.Name.StartsWith("A")).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Bitwise_And()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => (x.Id & 1) > 0).Select(x => new { x.Name, x.Age }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Equals_And_Not_Equals_Method()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age.Equals(20) && !x.Name.Equals("Johnny")).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_FullText_Includes()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Name.Includes("Johnny")).Select(x => new { x.Name, x.Age }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Varchar_Column_Uses_DbString()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Phone == "0912345678").Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Null_Comparisons()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Phone == null && x.Birthday != null).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Nullable_DateTime_Guid_Parameters()
        {
            var birthday = new DateTime(2000, 1, 2, 3, 4, 5);
            var token = new Guid("3f2504e0-4f89-11d3-9a0c-0305e82c3301");

            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Birthday >= birthday && x.Token == token).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Closure_Member_Access()
        {
            var criteria = new Member { Age = 18, Name = "Johnny" };

            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age == criteria.Age && x.Name == criteria.Name).Select(x => new { x.Id }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Boolean_Member()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.IsActive).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_String_CompareTo()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Name.CompareTo("M") > 0).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_And_Or_Chaining()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id == 1).Or(x => x.Id == 2).And(x => x.Age > 18).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_OrderBy_ThenByDescending()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age > 0).OrderBy(x => x.Id).ThenByDescending(x => x.Name).Select(x => new { x.Name, x.Age }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Distinct()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id > 0).Distinct(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Skip_Take()
        {
            return SqlSnapshot.MatchAsync(() => this.members.OrderBy(x => x.Id).Select(x => new { x.Name, x.Age }).Skip(20).Take(10).QueryAsync());
        }

        [TestMethod]
        public Task Query_Without_NoLock_When_Not_DirtyRead()
        {
            var dataAccess = SqlSnapshot.CreateDataAccess<Member>();

            dataAccess.IsDirtyRead = false;

            return SqlSnapshot.MatchAsync(() => dataAccess.Where(x => x.Id == 1).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_Explicit_Table_Name()
        {
            var dataAccess = SqlSnapshot.CreateDataAccess<Member>("tbl_member_archive");

            return SqlSnapshot.MatchAsync(() => dataAccess.Where(x => x.Id == 1).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Query_InnerJoin_Navigation()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                          .Where((x, y) => y.Name.Contains("業務"))
                          .Select((x, y) => new { x.Id, x.Name, DepartmentId = y.Id, DepartmentName = y.Name })
                          .QueryAsync());
        }

        [TestMethod]
        public Task Query_InnerJoin_Three_Tables()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                          .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                          .Where((x, y, z) => x.Age > 20)
                          .OrderBy((x, y, z) => z.Name)
                          .Select((x, y, z) => new { x.Id, x.Name, DepartmentId = y.Id, DepartmentName = y.Name, ManagerId = z.Id, ManagerName = z.Name })
                          .QueryAsync());
        }

        [TestMethod]
        public Task Query_LeftJoin_One_To_Many()
        {
            return SqlSnapshot.MatchAsync(
                () => this.members.LeftJoin(x => x.ManagedDepartments, (x, y) => x.Id == y.ManagerId)
                          .Where((x, y) => x.Age > 20)
                          .Select((x, y) => new { x.Id, x.Name, DepartmentId = y.Id, DepartmentName = y.Name })
                          .QueryAsync());
        }

        [TestMethod]
        public Task Query_GroupBy()
        {
            var statistics = SqlSnapshot.CreateDataAccess<MemberStatistics>();

            return SqlSnapshot.MatchAsync(
                () => statistics.Where(x => x.Name == "Johnny")
                          .GroupBy(x => new { x.Name }, g => new MemberStatistics { Name = g.Select(x => x.Name), AverageAge = g.Avg(x => x.Age) })
                          .QueryOneAsync());
        }

        [TestMethod]
        public Task Count()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age == 20).CountAsync());
        }

        [TestMethod]
        public Task Exists()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id == 1).ExistsAsync());
        }

        [TestMethod]
        public Task Exists_InnerJoin()
        {
            return SqlSnapshot.MatchAsync(() => this.members.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id).Where((x, y) => y.Name.Contains("業務")).ExistsAsync());
        }

        // ---- 以下記錄 3.x 已知的限制與陷阱（見 doc/redesign-plan-v4.md §2.3、§2.4） ----

        [TestMethod]
        public Task Pitfall_Second_Where_Overwrites_First()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age > 18).Where(x => x.Name == "Johnny").Select(x => new { x.Id }).QueryAsync());
        }

        [TestMethod]
        public Task Pitfall_Array_Length_Literal_Treated_As_Element()
        {
            // new int[0] 被當成 { 0 }，產生 [No] = 0，而不是 1 = 0。
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => new int[0].Contains(x.Id)).Select(x => new { x.Name }).QueryAsync());
        }

        [TestMethod]
        public Task Pitfall_Query_Without_Select_Throws()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Id == 1).QueryAsync());
        }

        [TestMethod]
        public Task Pitfall_Method_Call_On_Right_Side_Throws()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Birthday > DateTime.Today.AddDays(-1)).Select(x => new { x.Id }).QueryAsync());
        }

        [TestMethod]
        public Task Pitfall_Constant_On_Left_Side_Throws()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => 18 < x.Age).Select(x => new { x.Id }).QueryAsync());
        }

        [TestMethod]
        public Task Pitfall_Arithmetic_On_Column_Throws()
        {
            return SqlSnapshot.MatchAsync(() => this.members.Where(x => x.Age + 1 > 18).Select(x => new { x.Id }).QueryAsync());
        }
    }
}
