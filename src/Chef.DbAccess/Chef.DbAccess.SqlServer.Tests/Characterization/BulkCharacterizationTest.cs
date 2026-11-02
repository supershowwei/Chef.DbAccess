using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x BulkInsert／BulkUpdate／BulkUpsert 的 SQL 與參數快照。情境分別與 SqlServerDataAccessTest 的
    /// Insert.cs、Update.cs、Upsert.cs 中的 Bulk 測試一一對應，測試名稱相同。
    /// 只記錄 Bulk 方法本身（BulkUpsert 走新增與走更新的兩次都記錄）；驗證查詢與清除用的 DeleteAsync 不列入。
    /// 整合測試用亂數產生的 Id、名稱後綴與 DateTime.Now 在這裡改成固定值，快照才會穩定。
    /// </summary>
    [TestClass]
    [UsesVerify]
    public partial class BulkCharacterizationTest
    {
        private const string Suffix = "100";

        private static readonly DateTime RunningTime = new DateTime(2026, 11, 2, 9, 30, 0);

        [TestMethod]
        public async Task Test_BulkInsert_with_NonExistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkInsertAsync(
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1", IsActive = false },
                    new Club { Id = clubIds[1], Name = "TestClub2", IsActive = true }
                },
                x => x.Id == default);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkInsertAsync(
                () => new Club { Id = default(int), Name = default(string), RunningTime = default(DateTime?) },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1", RunningTime = RunningTime },
                    new Club { Id = clubIds[1], Name = "TestClub2", RunningTime = RunningTime }
                });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Output()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.BulkInsertAsync(
                new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Output_with_NonExistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.BulkInsertAsync(
                new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                x => new { x.Id, x.Name },
                x => x.Name == default);

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_QueryObject()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(() => new Club { Id = default(int), Name = default(string) })
                .BulkInsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_with_NonExistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkInsertAsync(
                () => new Club { Id = default(int), Name = default(string) },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                },
                x => x.Id == default);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_QueryObject_with_NonExistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(() => new Club { Id = default(int), Name = default(string) })
                .BulkInsertAsync(
                    new List<Club>
                    {
                        new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                    },
                    x => x.Id == default);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_Output_NonExistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.BulkInsertAsync(
                () => new IdentityTable { Name = default },
                new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                x => new { x.Id, x.Name },
                x => x.Name == default);

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_QueryObject_and_Output_NonExistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                .BulkInsertAsync(
                    new List<IdentityTable>
                    {
                        new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                    },
                    x => new { x.Id, x.Name },
                    x => x.Name == default);

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_Output()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.BulkInsertAsync(
                () => new IdentityTable { Name = default },
                new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_QueryObject_and_Output()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                .BulkInsertAsync(
                    new List<IdentityTable>
                    {
                        new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                    },
                    x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Dynamic_Setter()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(x => x.Id, default(int))
                .Set(x => x.Name, default(string))
                .BulkInsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_QueryObject_with_RequiredColumns()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkInsertAsync(
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1", IsActive = false },
                    new Club { Id = clubIds[1], Name = "TestClub2", IsActive = true }
                });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkUpdate()
        {
            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + Suffix },
                            new Club { Id = 16, Name = "羅怡君" + Suffix },
                            new Club { Id = 19, Name = "楊翊貴" + Suffix }
                        };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkUpdateAsync(x => x.Id == default(int), () => new Club { Name = default(string) }, clubs);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkUpdate_use_QueryObject()
        {
            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + Suffix },
                            new Club { Id = 16, Name = "羅怡君" + Suffix },
                            new Club { Id = 19, Name = "楊翊貴" + Suffix }
                        };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == default(int)).Set(() => new Club { Name = default(string) }).BulkUpdateAsync(clubs);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id >= 0 && x.Id <= 0,
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id == default(int),
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync_with_Properties_has_Same_Type()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id >= 0 && x.Id <= 0,
                () => new Club { Name = default, Intro = default },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1", Intro = "TestClub1_Intro" },
                    new Club { Id = clubIds[1], Name = "TestClub2", Intro = "TestClub2_Intro" }
                });

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id == default,
                () => new Club { Name = default, Intro = default },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub3", Intro = "TestClub3_Intro" },
                    new Club { Id = clubIds[1], Name = "TestClub4", Intro = "TestClub4_Intro" }
                });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync_use_Output()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id >= 0 && x.Id <= 0,
                () => new Club { Name = default(string) },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                },
                x => new { x.Id, x.Name });

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .BulkUpsertAsync(
                    new List<Club>
                    {
                        new Club { Id = clubIds[0], Name = "TestClub3" },
                        new Club { Id = clubIds[1], Name = "TestClub4" }
                    },
                    x => new { x.Id, x.Name });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync_use_QueryObject()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id >= default(int) && x.Id <= default(int))
                .Set(() => new Club { Name = default(string) })
                .BulkUpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .BulkUpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            await Verify(clubDataAccess.CapturedCommands);
        }
    }
}
