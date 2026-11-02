using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x DeleteAsync 的 SQL 與參數快照。情境與 SqlServerDataAccessTest.Delete.cs 一一對應，測試名稱相同。
    /// 只記錄要測的 DeleteAsync：整合測試事前清除與插入測試資料、驗證查詢、事後清除都不列入。
    /// Where(...).DeleteAsync() 內部呼叫的就是 DeleteAsync(predicate)，兩種寫法產生相同的 SQL。
    /// </summary>
    [TestClass]
    [UsesVerify]
    public partial class DeleteCharacterizationTest
    {
        [TestMethod]
        public async Task Test_DeleteAsync_with_Output()
        {
            var userDataAccess = new CapturingDataAccess<User>();

            await userDataAccess.Where(x => x.Id == 55)
                .DeleteAsync(
                    x => new
                    {
                        x.Id,
                        x.Name,
                        x.Age,
                        x.Phone,
                        x.Address
                    });

            await Verify(userDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Two_Tables()
        {
            var userDataAccess = new CapturingDataAccess<User>();

            await userDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id).Where((x, y) => y.Id == 3).DeleteAsync();

            await Verify(userDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Two_Tables_and_Output()
        {
            var userDataAccess = new CapturingDataAccess<User>();

            await userDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id)
                .Where((x, y) => y.Id == 3)
                .DeleteAsync(
                    x => new
                    {
                        x.Id,
                        x.Name,
                        x.Age,
                        x.Phone,
                        x.Address
                    });

            await Verify(userDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Three_Tables()
        {
            var userDataAccess = new CapturingDataAccess<User>();

            await userDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                .Where((x, y, z) => y.DepId == 2 && z.Id == 3)
                .DeleteAsync();

            await Verify(userDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Three_Tables_and_Output()
        {
            var userDataAccess = new CapturingDataAccess<User>();

            await userDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                .Where((x, y, z) => y.DepId == 2 && z.Id == 3)
                .DeleteAsync(
                    x => new
                    {
                        x.Id,
                        x.Name,
                        x.Age,
                        x.Phone,
                        x.Address
                    });

            await Verify(userDataAccess.CapturedCommands);
        }
    }
}
