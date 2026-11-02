using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x UpsertAsync 的 SQL 與參數快照。情境與 SqlServerDataAccessTest.Upsert.cs 一一對應，測試名稱相同。
    /// 整合測試先 Upsert 一次（走新增）、再 Upsert 一次（走更新），兩次都記錄；
    /// 驗證查詢、清除用的 DeleteAsync、把資料改回原值的 UpdateAsync 不列入。
    /// 整合測試用亂數產生的 Id 在這裡改成固定值，快照才會穩定。BulkUpsert 另外處理。
    /// </summary>
    [TestClass]
    [UsesVerify]
    public partial class UpsertCharacterizationTest
    {
        [TestMethod]
        public async Task Test_UpsertAsync()
        {
            var clubId = 101;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.UpsertAsync(x => x.Id == clubId, () => new Club { Name = "TestClub" });

            await clubDataAccess.UpsertAsync(
                x => x.Id == clubId && x.IsActive == true,
                () => new Club { Name = "TestClub997", IsActive = false });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_Output()
        {
            var clubId = 101;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.UpsertAsync(
                x => x.Id == clubId,
                () => new Club { Name = "TestClub" },
                x => new { x.Id, x.Name, x.IsActive });

            await clubDataAccess.Where(x => x.Id == clubId && x.IsActive == true)
                .Set(x => x.Name, "TestClub997")
                .Set(x => x.IsActive, false)
                .UpsertAsync(x => new { x.Id, x.Name, x.IsActive });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_Dynamic_Setter()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == 37).Set(x => x.Name, "test37name").Set(x => x.Intro, "test37intro").UpsertAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_Dynamic_Setter_on_Setter_Is_Null()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var queryObject = clubDataAccess.Where(x => x.Id == 37);

            queryObject = queryObject.Set(x => x.Name, "test37name").Set(x => x.Intro, "test37intro");

            // 整合測試在這裡呼叫的是 UpdateAsync，照原樣記錄
            await queryObject.UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_QueryObject()
        {
            var clubId = 101;

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == clubId).Set(() => new Club { Name = "TestClub" }).UpsertAsync();

            await clubDataAccess.Where(x => x.Id == clubId && x.IsActive == true)
                .Set(() => new Club { Name = "TestClub997", IsActive = false })
                .UpsertAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpsertAsync_Multiply()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.UpsertAsync(
                x => x.Id == default(int),
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            await clubDataAccess.UpsertAsync(
                x => x.Id == default(int),
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpsertAsync_Multiply_use_Output()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.UpsertAsync(
                x => x.Id == default(int),
                () => new Club { Name = default(string) },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                },
                x => new { x.Id, x.Name });

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .UpsertAsync(
                    new List<Club>
                    {
                        new Club { Id = clubIds[0], Name = "TestClub3" },
                        new Club { Id = clubIds[1], Name = "TestClub4" }
                    },
                    x => new { x.Id, x.Name });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpsertAsync_Multiply_use_QueryObject()
        {
            var clubIds = new[] { 101, 501 };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .UpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .UpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            await Verify(clubDataAccess.CapturedCommands);
        }
    }
}
