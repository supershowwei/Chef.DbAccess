using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer.Samples.Model.Data;
using Chef.DbAccess.Syntax;

namespace Chef.DbAccess.SqlServer.Samples
{
    public class RetrieveSample
    {
        private static async Task DemoQuery()
        {
            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();

            // SELECT 一筆 Id = 1 的 Member
            var member = await memberDataAccess
                             .Where(x => x.Id == 1)
                             .Select(x => new { x.Name, x.Age })
                             .QueryOneAsync();

            // SELECT 多筆 Id IN (1, 2, 3) 的 Member
            var members = await memberDataAccess
                              .Where(x => new[] { 1, 2, 3 }.Contains(x.Id))
                              .Select(x => new { x.Name, x.Age })
                              .QueryAsync();

            // SELECT 出來的結果再轉成任意類別
            var partialMembers = await memberDataAccess
                                     .Where(x => new[] { 1, 2, 3 }.Contains(x.Id))
                                     .Select(x => new { x.Id, x.Name, x.Age })
                                     .QueryAsync(x => new { x.Id, x.Name });

            // SELECT 出來的結果再傳入 Aggregate 函式
            var totalAge = await memberDataAccess
                               .Where(x => new[] { 1, 2, 3 }.Contains(x.Id))
                               .Select(x => new { x.Id, x.Name, x.Age })
                               .QueryAsync(0, (accuAge, m) => accuAge + m.Age);

            // SELECT 多筆 Id NOT IN (1, 2, 3) 的 Member
            members = await memberDataAccess
                          .Where(x => !new[] { 1, 2, 3 }.Contains(x.Id))
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // SELECT TOP 10 筆 Age >= 30 AND Name LIKE 'J%' ORDER BY Age 的 Member
            members = await memberDataAccess
                          .Where(x => x.Age >= 30 && x.Name.StartsWith("J"))
                          .OrderBy(x => x.Age)
                          .Take(10)
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // SELECT (Id & 1) > 0 的 Member
            members = await memberDataAccess
                          .Where(x => (x.Id & 1) > 0)
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // SELECT 多筆 Age = 18 AND Name LIKE '%y' 的 Member，並傳回 Name, Age 欄位。
            members = await memberDataAccess
                          .Where(x => x.Age == 18 && x.Name.EndsWith("y"))
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // SELECT 多筆 Name LIKE '%e%' ORDER BY Id, Name DESC 的 Member
            members = await memberDataAccess
                          .Where(x => x.Name.Contains("e"))
                          .OrderBy(x => x.Id)
                          .ThenByDescending(x => x.Name)
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // 從全文檢索（Full-Text Search）SELECT 多筆 Name 含有 'Johnny' 的 Member
            members = await memberDataAccess
                          .Where(x => x.Name.Includes("Johnny"))
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // SELECT 多筆 Name NOT LIKE '%e%' ORDER BY Id, Name DESC 的 Member
            members = await memberDataAccess
                          .Where(x => !x.Name.Contains("e"))
                          .OrderBy(x => x.Id)
                          .ThenByDescending(x => x.Name)
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // 計算 Age = 20 的 Member 數量
            var memberCount = await memberDataAccess
                                  .Where(x => x.Age == 20)
                                  .CountAsync();

            // 計算 Age = 20 的 Member 數量（使用 Equals()）
            memberCount = await memberDataAccess
                              .Where(x => x.Age.Equals(20))
                              .CountAsync();

            // 計算 Age <> 20 的 Member 數量（使用 Equals()）
            memberCount = await memberDataAccess
                              .Where(x => !x.Age.Equals(20))
                              .CountAsync();

            // 回傳 Id = 1 的 Member 是否存在？
            var memberExists = await memberDataAccess
                                   .Where(x => x.Id == 1)
                                   .ExistsAsync();

            // 回傳 Name 有 "業務" 字樣的 Department 是否有 Member 存在？
            memberExists = await memberDataAccess
                               .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                               .Where((x, y) => y.Name.Contains("業務"))
                               .ExistsAsync();

            // 使用 And() 方法 SELECT 多筆 Age = 18 AND Name LIKE '%y' 的 Member，並傳回 Name, Age 欄位。
            members = await memberDataAccess
                          .Where(x => x.Age == 18)
                          .And(x => x.Name.EndsWith("y"))
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // 使用 Or() 方法 SELECT 多筆 Id IN (1, 2, 3) 的 Member
            members = await memberDataAccess
                          .Where(x => x.Id == 1)
                          .Or(x => x.Id == 2)
                          .Or(x => x.Id == 3)
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

            // 使用 Distinct() 方法 SELECT 不重複的 Member 名字
            members = await memberDataAccess
                          .Where(x => x.Id > 0)
                          .Distinct(x => new { x.Name })
                          .QueryAsync();

            // 使用 Skip() + Take() 方法跳過 20 筆取 10 筆資料，可以用做分頁查詢。
            members = await memberDataAccess
                          .OrderBy(x => x.Id)
                          .Select(x => new { x.Name, x.Age })
                          .Skip(20)
                          .Take(10)
                          .QueryAsync();

            // 使用 InnerJoin() 找出年紀大於 20 歲的 Member，以及 Member 的 Department 和 Manager。
            members = await memberDataAccess
                          .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                          .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                          .Where((x, y, z) => x.Age > 20)
                          .Select(
                              (x, y, z) => new
                                           {
                                               x.Id,
                                               x.Name,
                                               DepartmentId = y.Id,
                                               DepartmentName = y.Name,
                                               ManagerId = z.Id,
                                               ManagerName = z.Name
                                           })
                          .QueryAsync();

            var memberStatisticsDataAccess = dataAccessFactory.Create<MemberStatistics>();

            // 使用 GroupBy 找出 Member 名字叫 Johnny 的平均年齡
            var statistics = await memberStatisticsDataAccess
                                 .Where(x => x.Name == "Johnny")
                                 .GroupBy(
                                     x => new { x.Name },
                                     g => new MemberStatistics
                                          {
                                              Name = g.Select(x => x.Name),
                                              AverageAge = g.Avg(x => x.Age)
                                          })
                                 .QueryOneAsync();
        }

        private static async Task DemoOneToManyQuery()
        {
            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();

            // 使用 InnerJoin() 找出年紀大於 20 歲的 Member，以及 Member 的 Manager 和目前所掌管的所有 ManagedDepartments。
            var members = await memberDataAccess.InnerJoin(x => x.ManagedDepartments, (x, y) => x.Id == y.ManagerId)
                              .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                              .Where((x, y, z) => x.Age > 20)
                              .Select(
                                  (x, y, z) => new
                                               {
                                                   x.Id,
                                                   x.Name,
                                                   DepartmentId = y.Id,
                                                   DepartmentName = y.Name,
                                                   ManagerId = z.Id,
                                                   ManagerName = z.Name
                                               })
                              .QueryAsync();
        }
    }
}