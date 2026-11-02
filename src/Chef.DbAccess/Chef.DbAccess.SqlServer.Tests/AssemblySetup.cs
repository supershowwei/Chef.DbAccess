using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;

namespace Chef.DbAccess.SqlServer.Tests
{
    [TestClass]
    public class AssemblySetup
    {
        [AssemblyInitialize]
        public static void Initialize(TestContext context)
        {
            // 快照統一放在測試程式碼旁邊的 Snapshots 資料夾，避免和 .cs 檔混在一起
            Verifier.UseSourceFileRelativeDirectory("Snapshots");

            // 測試模型的 [ConnectionString] 只寫名稱，連線字串在這裡集中註冊
            TestDatabase.RegisterConnectionStrings();
        }
    }
}
