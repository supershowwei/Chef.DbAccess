using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Chef.DbAccess.Benchmarks.Database;
using Chef.DbAccess.SqlServer;
using Microsoft.Data.SqlClient;

namespace Chef.DbAccess.Benchmarks.Write
{
    /// <summary>
    /// §10.2：大量新增 10 萬筆。§10.2 的條件是「不慢於 3.x 的 TVP」，所以以 3.x 為基準；Dapper 沒有大量寫入的 API。
    /// SqlBulkCopy 是 5.0 打算採用的做法，先列出來當參考數字。
    /// 一次呼叫就要數百毫秒以上，所以每輪只呼叫一次，每輪之前清空資料表。
    /// <para>
    /// 只寫入 Email 以外的 8 個欄位：3.x 建立 TVP 欄位時以「屬性型別」去除重複（StatementExtension.ToColumnDefinitions），
    /// 第二個同型別的屬性會被略過，INSERT 的欄位清單卻仍包含它，執行時出現 Invalid column name。
    /// Name 與 Email 都是 string，所以避開 Email，其餘 8 個欄位的型別彼此不同。SqlBulkCopy 寫入同樣的 8 個欄位。
    /// </para>
    /// </summary>
    [MemoryDiagnoser]
    [WarmupCount(2)]
    [IterationCount(10)]
    [InvocationCount(1)]
    public class BulkInsertBenchmark
    {
        private const int Count = 100_000;

        private IDataAccess<MemberWrite> dataAccess;

        // 在 GlobalSetup 先建好，不算進量測時間
        private List<MemberWrite> members;

        [GlobalSetup]
        public async Task Setup()
        {
            BenchmarkDatabase.EnsureCreated();

            this.dataAccess = SqlServerDataAccessFactory.Instance.Create<MemberWrite>();
            this.members = MemberWriteTable.CreateRows(Count);

            // 兩種寫法都不寫 Email，來源先設成 NULL，才能與讀回的資料比對
            foreach (var member in this.members) member.Email = null;

            await this.Check(nameof(this.Chef3x), this.Chef3x);
            await this.Check(nameof(this.SqlBulkCopy), this.SqlBulkCopy);

            MemberWriteTable.Truncate();
        }

        [IterationSetup]
        public void Truncate()
        {
            MemberWriteTable.Truncate();
        }

        [Benchmark(Baseline = true)]
        public Task<int> Chef3x()
        {
            // 以 setter 範本指定寫入的欄位，[Required] 的版本會包含 Email 而失敗
            return this.dataAccess.BulkInsertAsync(
                () => new MemberWrite
                      {
                          Name = default(string),
                          Age = default(int?),
                          Balance = default(decimal),
                          IsActive = default(bool),
                          CreatedAt = default(DateTime),
                          UpdatedAt = default(DateTime?),
                          Token = default(Guid),
                          DepartmentId = default(int)
                      },
                this.members);
        }

        /// <summary>
        /// 物件轉成 DataTable 也算在量測時間內：3.x 同樣在呼叫裡把物件轉成 SqlDataRecord。
        /// </summary>
        [Benchmark]
        public async Task<int> SqlBulkCopy()
        {
            var table = new DataTable();

            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("Age", typeof(int));
            table.Columns.Add("Balance", typeof(decimal));
            table.Columns.Add("IsActive", typeof(bool));
            table.Columns.Add("CreatedAt", typeof(DateTime));
            table.Columns.Add("UpdatedAt", typeof(DateTime));
            table.Columns.Add("Token", typeof(Guid));
            table.Columns.Add("DepartmentId", typeof(int));

            foreach (var member in this.members)
            {
                table.Rows.Add(
                    member.Name,
                    (object)member.Age ?? DBNull.Value,
                    member.Balance,
                    member.IsActive,
                    member.CreatedAt,
                    (object)member.UpdatedAt ?? DBNull.Value,
                    member.Token,
                    member.DepartmentId);
            }

            using (var bulkCopy = new Microsoft.Data.SqlClient.SqlBulkCopy(BenchmarkDatabase.ConnectionString) { DestinationTableName = "MemberWrite" })
            {
                // 預設依欄位順序對應，目的資料表的第一個欄位是 Id，所以要依名稱對應
                foreach (DataColumn column in table.Columns) bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);

                await bulkCopy.WriteToServerAsync(table);

                return bulkCopy.RowsCopied;
            }
        }

        // 從空的資料表開始，確認這個寫法寫入了全部的資料，而且每一筆都與來源相同
        private async Task Check(string name, Func<Task<int>> bulkInsert)
        {
            MemberWriteTable.Truncate();

            var affected = await bulkInsert();

            if (affected != Count) throw new InvalidOperationException($"{name}：影響筆數是 {affected}，預期 {Count}");

            ResultCheck.AssertSame(name, this.members, MemberWriteTable.ReadAll());
        }
    }
}
