using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    [TestClass]
    [UsesVerify]
    public partial class QueryCharacterizationTest
    {
        [TestMethod]
        public async Task Test_QueryAsync_with_Where_and_Select()
        {
            var clubDataAccess = new CapturingDataAccess<Club>();

            await clubDataAccess.Where(x => x.Id == 25).Select(x => new { x.Id, x.Name }).QueryAsync();

            await Verify(clubDataAccess.CapturedCommands);
        }
    }
}
