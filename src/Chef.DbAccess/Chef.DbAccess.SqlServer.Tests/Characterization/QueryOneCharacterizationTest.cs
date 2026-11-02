using System.Linq;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x QueryOneAsync 的 SQL 與參數快照。情境與 SqlServerDataAccessTest.QueryOne.cs 一一對應，測試名稱相同。
    /// 在產生 SQL 之前就丟出例外的情境沒有 SQL 可以記錄，不列入。
    /// </summary>
    [TestClass]
    [UsesVerify]
    public partial class QueryOneCharacterizationTest
    {
        // 與 SqlServerDataAccessTest 相同，給 use_this_Keyword 情境使用
        internal Club Club => new Club { Id = 25 };

        [TestMethod]
        public async Task Test_QueryOneAsync_use_String_Comparison()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.Where(x => x.Name.CompareTo("J") >= 0).Select(x => new { x.Id }).QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.QueryOneAsync(x => x.Id == 25, null, x => new { x.Name });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Selector()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.QueryOneAsync(x => x.Id == 25, selector: x => new { x.Name });

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Selector_use_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == 25).Select(x => new { x.Name }).QueryOneAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_use_Pagination_QueryObject()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(3).Take(1).QueryOneAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_AS_Keyword_Alias()
        {
            var advertisementSettingDataAccess = new CapturingDataAccess<AdvertisementSetting>("Advertisement");

            await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            await Verify(advertisementSettingDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_assign_ConnectionString_Name()
        {
            var advertisementSettingDataAccess = new CapturingDataAccess<AdvertisementSetting>("Advertisement2");

            await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            await Verify(advertisementSettingDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Multiple_ConnectionStringAttributes()
        {
            var advertisementSettingDataAccess = new CapturingDataAccess<DerivedAdvertisementSetting>("Advertisement");

            await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            await Verify(advertisementSettingDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_assign_ConnectionString_Name_from_DerivedClass()
        {
            var advertisementSettingDataAccess = new CapturingDataAccess<DerivedAdvertisementSetting>("Advertisement3");

            await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            await Verify(advertisementSettingDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_use_this_Keyword()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == this.Club.Id).Select(x => new { x.Name }).QueryOneAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Non_Existent_Prop_will_Throw_SqlException()
        {
            var userDataAccess = new CapturingDataAccess<User>();

            // 整合測試在資料庫端才失敗（Invalid column name），這裡記錄 3.x 送出的 SQL
            await userDataAccess.Where(x => x.Id == 1)
                .Select(x => new { x.Id, x.NonExistentProp })
                .QueryOneAsync();

            await Verify(userDataAccess.CapturedCommands);
        }
    }
}
