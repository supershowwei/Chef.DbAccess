using System;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.SqlServer.Tests
{
    /// <summary>
    /// 測試資料庫的連線字串。測試模型的 [ConnectionString] 只寫名稱，由這裡集中註冊到 SqlServerDataAccessFactory。
    /// </summary>
    internal static class TestDatabase
    {
        // 不含資料庫名稱的基底連線字串；沒有設定時使用 test/setup-localdb.ps1 在 LocalDB 建立的測試資料庫
        private const string BaseConnectionStringVariable = "CHEF_TEST_SQLSERVER";

        private const string LocalDbConnectionString = @"Data Source=(LocalDb)\MSSQLLocalDB;Integrated Security=True";

        public static void RegisterConnectionStrings()
        {
            var baseConnectionString = Environment.GetEnvironmentVariable(BaseConnectionStringVariable);

            if (string.IsNullOrEmpty(baseConnectionString)) baseConnectionString = LocalDbConnectionString;

            var factory = SqlServerDataAccessFactory.Instance;

            factory.AddConnectionString("Club", WithDatabase(baseConnectionString, "Club"));
            factory.AddConnectionString("Member", WithDatabase(baseConnectionString, "Member"));
            factory.AddConnectionString("Advertisement", WithDatabase(baseConnectionString, "Advertisement"));
            factory.AddConnectionString("Advertisement2", WithDatabase(baseConnectionString, "Advertisement"));
            factory.AddConnectionString("Advertisement3", WithDatabase(baseConnectionString, "Advertisement"));

            // 刻意指向兩台不同的假伺服器，給「跨伺服器 JOIN 會丟出例外」的測試使用，不會真的連線
            factory.AddConnectionString("WantGooConnection", @"Data Source=a.b.c.d;User ID=abc;Password=cba;Initial Catalog=AAA;Max Pool Size=50000");
            factory.AddConnectionString("MallConnection", @"Data Source=d.c.b.a;User ID=abc;Password=cba;Initial Catalog=BBB;Max Pool Size=50000");
        }

        // 3.x 用 Regex 從連線字串取出伺服器與資料庫名稱，要求值的後面接著分號，所以結尾一律補上分號
        private static string WithDatabase(string baseConnectionString, string database)
        {
            var builder = new SqlConnectionStringBuilder(baseConnectionString) { InitialCatalog = database };

            return builder.ConnectionString + ";";
        }
    }
}
