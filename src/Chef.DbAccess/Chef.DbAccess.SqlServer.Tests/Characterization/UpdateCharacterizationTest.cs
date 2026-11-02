using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x UpdateAsync 的 SQL 與參數快照。情境與 SqlServerDataAccessTest.Update.cs 一一對應，測試名稱相同。
    /// 只記錄要測的 UpdateAsync：整合測試的驗證查詢、以及把資料改回原值的 UpdateAsync 不列入。
    /// 整合測試用亂數產生的名稱後綴在這裡改成固定值，快照才會穩定。BulkUpdate 另外處理。
    /// </summary>
    [TestClass]
    [UsesVerify]
    public partial class UpdateCharacterizationTest
    {
        private const string Suffix = "100";

        [TestMethod]
        public async Task Test_UpdateAsync()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var clubName = "歐陽邦瑋" + Suffix;

            await clubDataAccess.UpdateAsync(x => x.Id.Equals(15), () => new Club { Name = clubName });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_set_Null()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.UpdateAsync(x => x.Id.Equals(36), () => new Club { Intro = null });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_use_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var clubName = "歐陽邦瑋" + Suffix;

            await clubDataAccess.Where(x => x.Id.Equals(15)).Set(() => new Club { Name = clubName }).UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_use_QueryObject_with_Duplicate_Set()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id.Equals(15)).Set(x => x.Name, "歐陽邦瑋99").Set(x => x.Name, "歐陽邦瑋123").UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_Join_Two_Tables_use_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var clubName = "歐陽邦瑋" + Suffix;

            await clubDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                .Where((x, y) => y.Id.Equals(15))
                .Set(() => new Club { Name = clubName })
                .UpdateAsync();

            await clubDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                .Where((x, y) => y.Id.Equals(15))
                .Set(x => x.Name, clubName)
                .UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_Join_Three_Tables_use_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var clubName = "歐陽邦瑋" + Suffix;

            await clubDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                .InnerJoin((x, y) => y.Self, (x, y, z) => y.Id == z.Id)
                .Where((x, y, z) => z.Id.Equals(15))
                .Set(() => new Club { Name = clubName })
                .UpdateAsync();

            await clubDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                .InnerJoin((x, y) => y.Self, (x, y, z) => y.Id == z.Id)
                .Where((x, y, z) => z.Id.Equals(15))
                .Set(x => x.Name, clubName)
                .UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_Join_Four_Tables_use_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var clubName = "歐陽邦瑋" + Suffix;

            await clubDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                .InnerJoin((x, y) => y.Self, (x, y, z) => y.Id == z.Id)
                .InnerJoin((x, y, z) => z.Self, (x, y, z, a) => z.Id == a.Id)
                .Where((x, y, z, a) => a.Id.Equals(15))
                .Set(() => new Club { Name = clubName })
                .UpdateAsync();

            await clubDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                .InnerJoin((x, y) => y.Self, (x, y, z) => y.Id == z.Id)
                .InnerJoin((x, y, z) => z.Self, (x, y, z, a) => z.Id == a.Id)
                .Where((x, y, z, a) => a.Id.Equals(15))
                .Set(x => x.Name, clubName)
                .UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_use_Dynamic_Setter()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            var clubName = "歐陽邦瑋" + Suffix;

            await clubDataAccess.Where(x => x.Id.Equals(15)).Set(x => x.Name, clubName).UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_use_QueryObject_set_Null()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == 36).Set(() => new Club { Intro = null }).UpdateAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_Multiply()
        {
            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + Suffix },
                            new Club { Id = 16, Name = "羅怡君" + Suffix },
                            new Club { Id = 19, Name = "楊翊貴" + Suffix }
                        };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.UpdateAsync(x => x.Id == default(int), () => new Club { Name = default(string) }, clubs);

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_Multiply_use_QueryObject()
        {
            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + Suffix },
                            new Club { Id = 16, Name = "羅怡君" + Suffix },
                            new Club { Id = 19, Name = "楊翊貴" + Suffix }
                        };

            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == default(int)).Set(() => new Club { Name = default(string) }).UpdateAsync(clubs);

            await Verify(clubDataAccess.CapturedCommands);
        }
    }
}
