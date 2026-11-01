using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.Tests
{
    [TestClass]
    public class ConnectionStringAttributeTest
    {
        [TestMethod]
        public void Test_Constructor_keeps_NameOrConnectionString_and_Schema_defaults_to_dbo()
        {
            var attribute = new ConnectionStringAttribute("Club");

            Assert.AreEqual("Club", attribute.ConnectionString);
            Assert.AreEqual("dbo", attribute.Schema);
        }
    }
}
