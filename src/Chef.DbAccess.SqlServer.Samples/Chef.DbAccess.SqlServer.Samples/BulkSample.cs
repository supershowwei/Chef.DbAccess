using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer.Samples.Model.Data;

namespace Chef.DbAccess.SqlServer.Samples
{
    public class BulkSample
    {
        private static async Task DemoBulk()
        {
            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();

            // 批次 INSERT，只有預設的必填欄位。
            await memberDataAccess.BulkInsertAsync(
                new List<Member>
                {
                    new Member { Id = 345, Name = "Logi" },
                    new Member { Id = 678, Name = "Alucky" }
                });

            // 批次 INSERT，新增 Id, Name, Phone, Age 欄位。
            await memberDataAccess
                .Set(() => new Member { Id = default(int), Name = default(string), Phone = default(string), Age = default(int) })
                .BulkInsertAsync(
                    new List<Member>
                    {
                        new Member { Id = 324, Name = "Cruzer", Phone = "0000-000123", Age = 12 },
                        new Member { Id = 537, Name = "Slice", Phone = "0000-000456", Age = 21 }
                    });

            // 批次 UPDATE Name, Phone 欄位
            await memberDataAccess
                .Where(x => x.Id == default(int))
                .Set(() => new Member { Name = default(string), Phone = default(string), })
                .BulkUpdateAsync(
                    new List<Member>
                    {
                        new Member { Id = 324, Name = "Lawson", Phone = "0000-000523" },
                        new Member { Id = 537, Name = "Marlon", Phone = "0000-000756" }
                    });

            // 批次 UPDATE or INSERT Name, Phone 欄位
            await memberDataAccess
                .Where(x => x.Id == default(int))
                .Set(() => new Member { Name = default(string), Phone = default(string), })
                .BulkUpsertAsync(
                    new List<Member>
                    {
                        new Member { Id = 324, Name = "Ramone", Phone = "0000-000623" },
                        new Member { Id = 657, Name = "Chris", Phone = "0000-000835" }
                    });

            // 批次 UPDATE or INSERT Name, Phone 欄位，回傳 Id 欄位。
            await memberDataAccess
                .Where(x => x.Id == default(int))
                .Set(() => new Member { Name = default(string), Phone = default(string), })
                .BulkUpsertAsync(
                    new List<Member>
                    {
                        new Member { Id = 324, Name = "Ramone", Phone = "0000-000623" },
                        new Member { Id = 657, Name = "Chris", Phone = "0000-000835" }
                    },
                    x => new { x.Id });
        }
    }
}