using System;
using System.Collections.Generic;
using Chef.DbAccess.Benchmarks.Database;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.Benchmarks.Write
{
    /// <summary>
    /// 寫入情境共用的 MemberWrite 資料表操作與資料。
    /// </summary>
    internal static class MemberWriteTable
    {
        // Id 以外的 9 個欄位，Id 由資料庫產生
        public const string Columns = "[Name], [Email], [Age], [Balance], [IsActive], [CreatedAt], [UpdatedAt], [Token], [DepartmentId]";

        public const string InsertSql = "INSERT INTO [MemberWrite] (" + Columns + @")
VALUES (@Name, @Email, @Age, @Balance, @IsActive, @CreatedAt, @UpdatedAt, @Token, @DepartmentId)";

        public static void Truncate()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                connection.Execute("TRUNCATE TABLE [MemberWrite]");
            }
        }

        public static List<MemberWrite> ReadAll()
        {
            using (var connection = new SqlConnection(BenchmarkDatabase.ConnectionString))
            {
                return connection.Query<MemberWrite>("SELECT [Id], " + Columns + " FROM [MemberWrite]").AsList();
            }
        }

        /// <summary>
        /// 產生要寫入的資料：分布與 Member 的種子資料相同（可為 NULL 的欄位每 3 筆有 1 筆是 NULL），以固定的亂數種子產生。
        /// 時間都是整分鐘：3.x 的 TVP 把 DateTime 宣告成 DATETIME（精確度約 3 毫秒），整分鐘才能與其他寫法比對。
        /// </summary>
        public static List<MemberWrite> CreateRows(int count)
        {
            var random = new Random(2);
            var start = new DateTime(2026, 1, 1);
            var bytes = new byte[16];
            var rows = new List<MemberWrite>(count);

            for (var i = 1; i <= count; i++)
            {
                var isNull = i % 3 == 0;

                random.NextBytes(bytes);

                rows.Add(
                    new MemberWrite
                    {
                        Name = $"Member {i}",
                        Email = isNull ? null : $"member{i}@example.com",
                        Age = isNull ? (int?)null : random.Next(18, 80),
                        Balance = random.Next(0, 1_000_000) / 100m,
                        IsActive = i % 2 == 0,
                        CreatedAt = start.AddMinutes(i),
                        UpdatedAt = isNull ? (DateTime?)null : start.AddMinutes(i).AddDays(1),
                        Token = new Guid(bytes),
                        DepartmentId = random.Next(1, BenchmarkDatabase.DepartmentCount + 1)
                    });
            }

            return rows;
        }
    }
}
