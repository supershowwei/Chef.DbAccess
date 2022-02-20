using System.Collections.Generic;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer.Samples.Model.Data;

namespace Chef.DbAccess.SqlServer.Samples
{
    public class CreateSample
    {
        private static async Task DemoInsert()
        {
            var member = new Member { Id = 99, Name = "Kevin", Phone = "0000-000000", Age = 88 };

            var members = new List<Member>
                          {
                              new Member { Id = 98, Name = "Bob", Phone = "0000-000001", Age = 38 },
                              new Member { Id = 97, Name = "Tom", Phone = "0000-000002", Age = 85 }
                          };

            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();

            // INSERT 一筆 Member，僅預設的必填欄位。
            await memberDataAccess.InsertAsync(member);

            // INSERT 多筆 Member，僅預設的必填欄位。
            await memberDataAccess.InsertAsync(members);

            // INSERT 一筆 Member，使用 Fluent 語法，僅 Id, Name, Phone 欄位。
            await memberDataAccess
                .Set(() => new Member { Id = 100, Name = "Pass", Phone = "0978-878787" })
                .InsertAsync();

            // INSERT 一筆 Member，使用 Fluent 語法，僅 Id, Name, Phone 欄位，回傳 Id 欄位。
            var outputMember = await memberDataAccess
                                   .Set(() => new Member { Id = 100, Name = "Pass", Phone = "0978-878787" })
                                   .InsertAsync(x => new { x.Id });

            // 上述語法也可以寫成下面這樣
            await memberDataAccess
                .Set(m => m.Id, 100)
                .Set(m => m.Name, "Pass")
                .Set(m => m.Phone, "0978-878787")
                .InsertAsync();

            // INSERT 多筆 Member，使用 Fluent 語法，僅 Id, Name, Age 欄位。
            await memberDataAccess.Set(() => new Member { Id = default(int), Name = default(string), Age = default(int) })
                .InsertAsync(
                    new List<Member>
                    {
                        new Member { Id = 11, Name = "Joe", Phone = "0000-000003", Age = 23 },
                        new Member { Id = 22, Name = "Steve", Phone = "0000-000004", Age = 45 }
                    });

            // 上述語法也可以寫成下面這樣
            await memberDataAccess.Set(m => m.Id, default(int))
                .Set(m => m.Name, default(string))
                .Set(m => m.Age, default(int))
                .InsertAsync(
                    new List<Member>
                    {
                        new Member { Id = 11, Name = "Joe", Phone = "0000-000003", Age = 23 },
                        new Member { Id = 22, Name = "Steve", Phone = "0000-000004", Age = 45 }
                    });

            // 當 nonexistence 參數的條件不存在時，才 INSERT 資料。
            var result = await memberDataAccess
                .Set(() => new Member { Id = 11, Name = "Joe", Phone = "0000-000003", Age = 23 })
                .InsertAsync(x => x.Id == 11);
        }
    }
}