using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer.Samples.Model.Data;

namespace Chef.DbAccess.SqlServer.Samples
{
    public class UpsertSample
    {
        private static async Task DemoUpsert()
        {
            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();

            // UPDATE or INSERT 一筆 Id = 1 的 Member，包含 Name, Phone 欄位
            await memberDataAccess
                .Set(() => new Member { Name = "Black", Phone = "02-56567788" })
                .Where(x => x.Id == 1).UpsertAsync();

            // UPDATE or INSERT 一筆 Id = 1 的 Member，包含 Name, Phone 欄位，回傳 Id 欄位。
            var outputMember = await memberDataAccess
                                   .Set(() => new Member { Name = "Black", Phone = "02-56567788" })
                                   .Where(x => x.Id == 1)
                                   .UpsertAsync(x => new { x.Id });

            // 上述語法也可以寫成下面這樣
            await memberDataAccess
                .Set(m => m.Name, "Black")
                .Set(m => m.Phone, "02-56567788")
                .Where(x => x.Id == 1).UpsertAsync();

            // UPDATE or INSERT 多筆 Member，包含 Age, Phone 欄位
            await memberDataAccess.Set(() => new Member { Name = default(string), Age = default(int), Phone = default(string) })
                .Where(x => x.Id == default(int))
                .UpdateAsync(
                    new List<Member>
                    {
                        new Member { Id = 77, Phone = "0000-000066", Age = 36 },
                        new Member { Id = 88, Phone = "0000-000077", Age = 57 }
                    });

            // 上述語法也可以寫成下面這樣
            await memberDataAccess.Set(m => m.Name, default(string))
                .Set(m => m.Age, default(int))
                .Set(m => m.Phone, default(string))
                .Where(x => x.Id == default(int))
                .UpdateAsync(
                    new List<Member>
                    {
                        new Member { Id = 77, Phone = "0000-000066", Age = 36 },
                        new Member { Id = 88, Phone = "0000-000077", Age = 57 }
                    });
        }
    }
}