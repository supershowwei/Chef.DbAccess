using System;
using System.Data;
using Chef.DbAccess.SqlServer;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.Benchmarks.Database
{
    /// <summary>
    /// 基準用的資料庫 ChefBenchmark：與測試資料庫分開，資料量固定，由程式自己建立。
    /// </summary>
    internal static class BenchmarkDatabase
    {
        public const string Name = "ChefBenchmark";

        public const int CompanyCount = 10;
        public const int DepartmentCount = 100;
        public const int MemberCount = 10_000;

        // Dapper 的手寫 SQL 共用的欄位清單（Member 的 10 個欄位）
        public const string MemberColumns = "[Id], [Name], [Email], [Age], [Balance], [IsActive], [CreatedAt], [UpdatedAt], [Token], [DepartmentId]";

        // 不含資料庫名稱的基底連線字串；沒有設定時使用 LocalDB
        private const string BaseConnectionStringVariable = "CHEF_BENCHMARK_SQLSERVER";

        private const string LocalDbConnectionString = @"Data Source=(LocalDb)\MSSQLLocalDB;Integrated Security=True";

        // 資料表結構或種子資料改變時加一，既有的基準資料庫會被刪除重建
        private const int SchemaVersion = 2;

        private static readonly object SyncRoot = new object();

        private static string connectionString;

        public static string ConnectionString => connectionString ?? throw new InvalidOperationException("尚未呼叫 EnsureCreated。");

        /// <summary>
        /// 確認基準資料庫存在且版本正確（否則重建），並把連線字串註冊給 3.x 的 SqlServerDataAccessFactory。
        /// BenchmarkDotNet 每個情境都在獨立的處理程序執行，所以每個處理程序都會呼叫一次。
        /// </summary>
        public static void EnsureCreated()
        {
            lock (SyncRoot)
            {
                if (connectionString != null) return;

                var baseConnectionString = Environment.GetEnvironmentVariable(BaseConnectionStringVariable);

                if (string.IsNullOrEmpty(baseConnectionString)) baseConnectionString = LocalDbConnectionString;

                var database = WithDatabase(baseConnectionString, Name);

                if (GetSchemaVersion(baseConnectionString, database) != SchemaVersion)
                {
                    Recreate(WithDatabase(baseConnectionString, "master"), database);
                }

                // 3.x 用 Regex 從連線字串取出伺服器與資料庫名稱，要求值的後面接著分號
                SqlServerDataAccessFactory.Instance.AddConnectionString(Name, database + ";");

                connectionString = database;
            }
        }

        private static string WithDatabase(string baseConnectionString, string database)
        {
            return new SqlConnectionStringBuilder(baseConnectionString) { InitialCatalog = database }.ConnectionString;
        }

        private static int GetSchemaVersion(string baseConnectionString, string database)
        {
            using (var connection = new SqlConnection(WithDatabase(baseConnectionString, "master")))
            {
                connection.Open();

                if (Scalar(connection, $"SELECT DB_ID(N'{Name}')") is DBNull) return 0;
            }

            using (var connection = new SqlConnection(database))
            {
                connection.Open();

                var version = Scalar(connection, "IF OBJECT_ID(N'dbo.SchemaVersion') IS NOT NULL SELECT MAX([Version]) FROM [SchemaVersion] ELSE SELECT 0");

                return version is DBNull ? 0 : Convert.ToInt32(version);
            }
        }

        private static void Recreate(string master, string database)
        {
            using (var connection = new SqlConnection(master))
            {
                connection.Open();

                Execute(
                    connection,
                    $@"
IF DB_ID(N'{Name}') IS NOT NULL
BEGIN
    ALTER DATABASE [{Name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{Name}];
END
CREATE DATABASE [{Name}];
ALTER DATABASE [{Name}] SET RECOVERY SIMPLE;");
            }

            // 新建立的資料庫不能沿用連線集區裡指向舊資料庫的連線
            SqlConnection.ClearAllPools();

            using (var connection = new SqlConnection(database))
            {
                connection.Open();

                Execute(
                    connection,
                    @"
CREATE TABLE [Company] (
    [Id] INT NOT NULL PRIMARY KEY,
    [Name] NVARCHAR(50) NOT NULL);

CREATE TABLE [Department] (
    [Id] INT NOT NULL PRIMARY KEY,
    [CompanyId] INT NOT NULL,
    [Name] NVARCHAR(50) NOT NULL);

CREATE TABLE [Member] (
    [Id] INT NOT NULL PRIMARY KEY,
    [Name] NVARCHAR(50) NOT NULL,
    [Email] VARCHAR(100) NULL,
    [Age] INT NULL,
    [Balance] DECIMAL(18, 2) NOT NULL,
    [IsActive] BIT NOT NULL,
    [CreatedAt] DATETIME2 NOT NULL,
    [UpdatedAt] DATETIME2 NULL,
    [Token] UNIQUEIDENTIFIER NOT NULL,
    [DepartmentId] INT NOT NULL);

-- 寫入情境專用，欄位與 Member 相同，Id 由資料庫產生；查詢情境的 Member 資料因此不會被改動
CREATE TABLE [MemberWrite] (
    [Id] INT NOT NULL IDENTITY PRIMARY KEY,
    [Name] NVARCHAR(50) NOT NULL,
    [Email] VARCHAR(100) NULL,
    [Age] INT NULL,
    [Balance] DECIMAL(18, 2) NOT NULL,
    [IsActive] BIT NOT NULL,
    [CreatedAt] DATETIME2 NOT NULL,
    [UpdatedAt] DATETIME2 NULL,
    [Token] UNIQUEIDENTIFIER NOT NULL,
    [DepartmentId] INT NOT NULL);

CREATE TABLE [SchemaVersion] ([Version] INT NOT NULL);");

                BulkCopy(connection, CreateCompanies());
                BulkCopy(connection, CreateDepartments());
                BulkCopy(connection, CreateMembers());

                Execute(connection, $"INSERT INTO [SchemaVersion] ([Version]) VALUES ({SchemaVersion})");
            }
        }

        // 公司 1～10，每家公司 10 個部門
        private static DataTable CreateCompanies()
        {
            var table = new DataTable("Company");

            table.Columns.Add("Id", typeof(int));
            table.Columns.Add("Name", typeof(string));

            for (var i = 1; i <= CompanyCount; i++) table.Rows.Add(i, $"公司 {i}");

            return table;
        }

        private static DataTable CreateDepartments()
        {
            var table = new DataTable("Department");

            table.Columns.Add("Id", typeof(int));
            table.Columns.Add("CompanyId", typeof(int));
            table.Columns.Add("Name", typeof(string));

            for (var i = 1; i <= DepartmentCount; i++) table.Rows.Add(i, (i - 1) / (DepartmentCount / CompanyCount) + 1, $"部門 {i}");

            return table;
        }

        // 欄位型別與 MaterializationBenchmark 相同；可為 NULL 的欄位每 3 筆有 1 筆是 NULL，資料以固定的亂數種子產生
        private static DataTable CreateMembers()
        {
            var table = new DataTable("Member");

            table.Columns.Add("Id", typeof(int));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("Email", typeof(string));
            table.Columns.Add("Age", typeof(int));
            table.Columns.Add("Balance", typeof(decimal));
            table.Columns.Add("IsActive", typeof(bool));
            table.Columns.Add("CreatedAt", typeof(DateTime));
            table.Columns.Add("UpdatedAt", typeof(DateTime));
            table.Columns.Add("Token", typeof(Guid));
            table.Columns.Add("DepartmentId", typeof(int));

            var random = new Random(1);
            var start = new DateTime(2026, 1, 1);
            var bytes = new byte[16];

            for (var i = 1; i <= MemberCount; i++)
            {
                var isNull = i % 3 == 0;

                random.NextBytes(bytes);

                table.Rows.Add(
                    i,
                    $"Member {i}",
                    isNull ? (object)DBNull.Value : $"member{i}@example.com",
                    isNull ? (object)DBNull.Value : random.Next(18, 80),
                    random.Next(0, 1_000_000) / 100m,
                    i % 2 == 0,
                    start.AddMinutes(i),
                    isNull ? (object)DBNull.Value : start.AddMinutes(i).AddDays(1),
                    new Guid(bytes),
                    random.Next(1, DepartmentCount + 1));
            }

            return table;
        }

        private static void BulkCopy(SqlConnection connection, DataTable table)
        {
            using (var bulkCopy = new SqlBulkCopy(connection) { DestinationTableName = table.TableName })
            {
                bulkCopy.WriteToServer(table);
            }
        }

        private static object Scalar(SqlConnection connection, string sql)
        {
            using (var command = new SqlCommand(sql, connection))
            {
                return command.ExecuteScalar();
            }
        }

        private static void Execute(SqlConnection connection, string sql)
        {
            using (var command = new SqlCommand(sql, connection))
            {
                command.ExecuteNonQuery();
            }
        }
    }
}
