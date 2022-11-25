using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer.Samples.Model.Data;

namespace Chef.DbAccess.SqlServer.Samples
{
    public class UpdateSample
    {
        private static async Task DemoUpdate()
        {
            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();

            // UPDATE 一筆 Id = 1 的 Member Phone 欄位
            await memberDataAccess
                .Set(() => new Member { Phone = "02-87871234" })
                .Where(x => x.Id == 1)
                .UpdateAsync();

            // 動態 UPDATE 一筆 Id = 1 的 Member Age, Phone 欄位
            await memberDataAccess
                .Set(x => x.Age, 20)
                .Set(x => x.Phone, "02-87871234")
                .Where(x => x.Id == 1)
                .UpdateAsync();

            // 動態 UPDATE Department Name 有 "業務" 字樣的 Member Age, Phone 欄位
            await memberDataAccess
                .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                .Set(x => x.Age, 20)
                .Set(x => x.Phone, "02-87871234")
                .Where((x, y) => y.Name.Contains("業務"))
                .UpdateAsync();

            // UPDATE 多筆 Member 的 Age, Phone 欄位
            await memberDataAccess
                .Set(() => new Member { Age = default(int), Phone = default(string) })
                .Where(x => x.Id == default(int))
                .UpdateAsync(
                    new List<Member>
                    {
                        new Member { Id = 44, Phone = "0000-000006", Age = 63 },
                        new Member { Id = 55, Phone = "0000-000007", Age = 75 }
                    });

            // 上述語法也可以寫成下面這樣
            await memberDataAccess
                .Set(m => m.Age, default(int))
                .Set(m => m.Phone, default(string))
                .Where(x => x.Id == default(int))
                .UpdateAsync(
                    new List<Member>
                    {
                        new Member { Id = 44, Phone = "0000-000006", Age = 63 },
                        new Member { Id = 55, Phone = "0000-000007", Age = 75 }
                    });
        }
    }
}