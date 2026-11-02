using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x QueryAsync 的 SQL 與參數快照。情境與 SqlServerDataAccessTest.Query.cs 一一對應，測試名稱相同。
    /// </summary>
    [TestClass]
    [UsesVerify]
    public partial class QueryCharacterizationTest
    {
        private static readonly Regex DateTimeParameterRegex = new Regex(@"= \S+ \(DateTime\)$");

        [TestMethod]
        public async Task Test_QueryAsync_with_Where_and_Select()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == 25).Select(x => new { x.Id, x.Name }).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_GroupBy_and_OrderBy()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.GroupBy(x => new { x.Id }, g => new Club { Id = g.Select(x => x.Id) })
                .OrderBy(x => x.Id)
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Null()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Intro == null).Select(x => new { x.Id }).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.QueryAsync(x => new[] { 17, 25 }.Contains(x.Id), selector: x => new { x.Name });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_And_Operator()
        {
            var userDataAccess = new CapturingDataAccess<User>();

            await userDataAccess.Where(x => (x.DepartmentId & x.ManagerId) == 0).Select(x => new { x.Id, x.Name }).QueryAsync();
            await userDataAccess.Where(x => (x.DepartmentId & x.ManagerId) > 0).Select(x => new { x.Id, x.Name }).QueryAsync();

            await Verify(userDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_List_Contains_with_And()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var ids = new List<int> { 17, 25 };
            var active = true;

            await clubDataAccess.Where(x => ids.Contains(x.Id))
                .And(x => x.IsActive == active)
                .Select(x => new { x.Name })
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_Empty_List_Contains()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var ids = new List<int>();

            await clubDataAccess.Where(x => ids.Contains(x.Id)).Select(x => new { x.Name }).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_use_Pagination_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(3).Take(2).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id)).Select(x => new { x.Name }).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_use_Pagination_only_Skip_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(26).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_has_DateTime_Now()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.RunningTime < DateTime.Now).Select(x => new { x.Id, x.Name }).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands).ScrubLinesWithReplace(ScrubDateTimeNow);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_And()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.RunningTime < DateTime.Now)
                .And(y => y.IsActive == false)
                .Select(x => new { x.Id, x.Name })
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands).ScrubLinesWithReplace(ScrubDateTimeNow);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_Or()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.RunningTime > DateTime.Now)
                .Or(y => y.Name == "王真希")
                .Select(x => new { x.Id, x.Name })
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands).ScrubLinesWithReplace(ScrubDateTimeNow);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_OrderByDescending()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                .OrderByDescending(x => x.Id)
                .Select(x => new { x.Name })
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_Take_without_Ordering()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                .Select(x => new { x.Name })
                .Take(1)
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_OrderByDescending_and_Take()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                .OrderByDescending(x => x.Id)
                .Select(x => new { x.Name })
                .Take(1)
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAllAsync_with_Selector_use_QueryObject_and_OrderByDescending_and_ThenBy_Take()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.OrderByDescending(x => x.IsActive)
                .ThenBy(x => x.Id)
                .Select(x => new { x.Id, x.Name })
                .Take(1)
                .QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        // DateTime.Now 每次執行都不同，只保留型別
        private static string ScrubDateTimeNow(string line)
        {
            return DateTimeParameterRegex.Replace(line, "= {DateTime.Now} (DateTime)");
        }
    }
}
