using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace Chef.DbAccess.SqlServer.Tests
{
    /// <summary>
    /// 測試資料庫的連線字串。測試模型的 [ConnectionString] 只寫名稱，由這裡集中註冊到 SqlServerDataAccessFactory。
    /// </summary>
    internal static class TestDatabase
    {
        // 不含資料庫名稱的基底連線字串；設成 container 時改用 Testcontainers 啟動 SQL Server，
        // 沒有設定時使用 test/setup-localdb.ps1 在 LocalDB 建立的測試資料庫
        private const string BaseConnectionStringVariable = "CHEF_TEST_SQLSERVER";

        private const string UseContainer = "container";

        private const string LocalDbConnectionString = @"Data Source=(LocalDb)\MSSQLLocalDB;Integrated Security=True";

        private const string ContainerImage = "mcr.microsoft.com/mssql/server:2022-latest";

        private static readonly Regex BatchSeparatorRegex = new Regex(@"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);

        private static MsSqlContainer container;

        public static async Task InitializeAsync()
        {
            var baseConnectionString = Environment.GetEnvironmentVariable(BaseConnectionStringVariable);

            if (string.Equals(baseConnectionString, UseContainer, StringComparison.OrdinalIgnoreCase))
            {
                container = new MsSqlBuilder(ContainerImage).Build();

                await container.StartAsync();

                baseConnectionString = container.GetConnectionString();

                await CreateSampleDatabasesAsync(baseConnectionString);
            }
            else if (string.IsNullOrEmpty(baseConnectionString))
            {
                baseConnectionString = LocalDbConnectionString;
            }

            RegisterConnectionStrings(baseConnectionString);
        }

        public static async Task DisposeAsync()
        {
            if (container != null) await container.DisposeAsync();
        }

        private static void RegisterConnectionStrings(string baseConnectionString)
        {
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

        // 與 test/setup-localdb.ps1 執行同一份 SampleDB.sql；GO 是 sqlcmd 的批次分隔符號，要自己切開後逐批執行
        private static async Task CreateSampleDatabasesAsync(string connectionString)
        {
            var script = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "SampleDB.sql"));

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                foreach (var batch in BatchSeparatorRegex.Split(script).Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    using (var command = new SqlCommand(batch, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }
    }
}
