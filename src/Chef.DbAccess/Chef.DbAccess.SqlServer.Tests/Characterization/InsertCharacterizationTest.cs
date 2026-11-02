using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x InsertAsync 的 SQL 與參數快照。情境與 SqlServerDataAccessTest.Insert.cs 一一對應，測試名稱相同。
    /// 只記錄 InsertAsync 本身：整合測試之後的查詢是驗證、DeleteAsync 是清除（Delete 另有快照），
    /// 而且帶 Output 的情境要用 InsertAsync 的回傳值，攔截時拿不到。
    /// 整合測試用亂數產生的 Id 在這裡改成固定值，快照才會穩定。BulkInsert 另外處理。
    /// </summary>
    [TestClass]
    [UsesVerify]
    public partial class InsertCharacterizationTest
    {
        [TestMethod]
        public async Task Test_InsertAsync_with_Nonxistence()
        {
            var clubId = 29;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(new Club { Id = clubId, Name = "TestClub" }, x => x.Id == clubId);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_QueryObject_with_Nonxistence()
        {
            var clubId = 29;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(() => new Club { Id = clubId, Name = "TestClub" }).InsertAsync(x => x.Id == clubId);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_DeleteAsync_with_Nonxistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.InsertAsync(
                new IdentityTable { Name = "Johnny" },
                x => new { x.Id, x.Name },
                x => x.Name == "Johnny");

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_QueryObject_and_DeleteAsync_with_Nonxistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.Set(() => new IdentityTable { Name = "Johnny" })
                .InsertAsync(x => new { x.Id, x.Name }, x => x.Name == "Johnny");

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync()
        {
            var clubId = 101;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(new Club { Id = clubId, Name = "TestClub" });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_DeleteAsync()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.InsertAsync(
                new IdentityTable { Name = "Johnny" },
                x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Setter_with_Nonxistence()
        {
            var clubId = 29;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(() => new Club { Id = clubId, Name = "TestClub" }, x => x.Id == clubId);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Setter()
        {
            var clubId = 101;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(() => new Club { Id = clubId, Name = "TestClub" });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Setter_and_Output()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.InsertAsync(
                () => new IdentityTable { Name = "Johnny" },
                x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Setter_and_Output_with_Nonxistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.InsertAsync(
                () => new IdentityTable { Name = "Johnny" },
                x => new { x.Id, x.Name },
                x => x.Name == "Johnny");

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Dynamic_Setter()
        {
            var clubId = 101;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(x => x.Id, clubId).Set(x => x.Name, "TestClub").InsertAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Dynamic_Setter_and_Output()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.Set(x => x.Name, "Johnny").InsertAsync(x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_QueryObject()
        {
            var clubId = 101;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(() => new Club { Id = clubId, Name = "TestClub" }).InsertAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(
                new List<Club>
                {
                    new Club { Id = clubIds[1], Name = "TestClub999", IsActive = true }, new Club { Id = clubIds[0], Name = "TestClub998" }
                });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_Multiply_with_Nonxistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(
                new List<Club>
                {
                    new Club { Id = clubIds[1], Name = "TestClub999", IsActive = true },
                    new Club { Id = clubIds[0], Name = "TestClub998" }
                },
                x => x.Id == default);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_Multiply_use_QueryObject_with_Nonxistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(() => new Club { Id = default, Name = default, IsActive = default })
                .InsertAsync(
                    new List<Club>
                    {
                        new Club { Id = clubIds[1], Name = "TestClub999", IsActive = true },
                        new Club { Id = clubIds[0], Name = "TestClub998" }
                    },
                    x => x.Id == default);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_and_Output()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.InsertAsync(
                new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_and_Output_with_Nonxistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.InsertAsync(
                new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                x => new { x.Id, x.Name },
                x => x.Name == default);

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_QueryObject_and_Output_with_Nonxistence()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                .InsertAsync(
                    new List<IdentityTable>
                    {
                        new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                    },
                    x => new { x.Id, x.Name },
                    x => x.Name == default);

            await Verify(identityTableDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_Multiply_use_Setter_with_Nonxistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(
                () => new Club { Id = default(int), Name = default(string) },
                new List<Club>
                {
                    new Club { Id = clubIds[1], Name = "TestClub999" }, new Club { Id = clubIds[0], Name = "TestClub998" }
                },
                x => x.Id == default);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_Setter()
        {
            var clubIds = new[] { 2001, 3001 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.InsertAsync(
                () => new Club { Id = default(int), Name = default(string) },
                new List<Club> { new Club { Id = clubIds[1], Name = "TestClub999" }, new Club { Id = clubIds[0], Name = "TestClub998" } });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_Setter_and_QueryObject()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Set(() => new Club { Id = default(int), Name = default(string) })
                .InsertAsync(new List<Club> { new Club { Id = clubIds[1], Name = "TestClub999" }, new Club { Id = clubIds[0], Name = "TestClub998" } });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_QueryObject_and_Output()
        {
            var identityTableDataAccess = new CapturingDataAccess<IdentityTable>();

            await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                .InsertAsync(
                    new List<IdentityTable>
                    {
                        new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                    },
                    x => new { x.Id, x.Name });

            await Verify(identityTableDataAccess.CapturedCommands);
        }
    }
}
