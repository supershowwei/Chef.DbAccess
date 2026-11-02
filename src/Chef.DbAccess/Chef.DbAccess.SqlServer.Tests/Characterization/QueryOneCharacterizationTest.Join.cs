using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    public partial class QueryOneCharacterizationTest
    {
        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Two_Tables_OneToMany_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Departments, (x, y) => x.DepartmentId == y.DepId)
                .Where((x, y) => new[] { 1 }.Contains(x.Id))
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Three_Tables_OneToMany_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                .LeftJoin((x, y) => x.Departments, (x, y, z) => x.DepartmentId == z.DepId)
                .Where((x, y, z) => new[] { 1 }.Contains(x.Id))
                .Select(
                    (x, y, z) => new
                                 {
                                     x.Id,
                                     SubordinateId = y.Id,
                                     z.DepId,
                                     x.Name,
                                     SubordinateName = y.Name,
                                     DepartmentName = z.Name
                                 })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Four_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                .LeftJoin((x, y, z) => z.Subordinates, (x, y, z, m) => z.Id == m.ManagerId)
                .Where((x, y, z, m) => x.Id == 2)
                .Select(
                    (x, y, z, m) => new
                    {
                        x.Id,
                        x.Name,
                        Level1SubordinateId = y.Id,
                        Level1SubordinateName = y.Name,
                        Level2SubordinateId = z.Id,
                        Level2SubordinateName = z.Name,
                        Level3SubordinateId = m.Id,
                        Level3SubordinateName = m.Name
                    })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Five_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                .LeftJoin((x, y, z) => z.Subordinates, (x, y, z, m) => z.Id == m.ManagerId)
                .LeftJoin((x, y, z, m) => m.Subordinates, (x, y, z, m, n) => m.Id == n.ManagerId)
                .Where((x, y, z, m, n) => x.Id == 2)
                .Select(
                    (x, y, z, m, n) => new
                    {
                        x.Id,
                        x.Name,
                        Level1SubordinateId = y.Id,
                        Level1SubordinateName = y.Name,
                        Level2SubordinateId = z.Id,
                        Level2SubordinateName = z.Name,
                        Level3SubordinateId = m.Id,
                        Level3SubordinateName = m.Name,
                        Level4SubordinateId = n.Id,
                        Level4SubordinateName = n.Name
                    })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Six_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                .LeftJoin((x, y, z) => z.Subordinates, (x, y, z, m) => z.Id == m.ManagerId)
                .LeftJoin((x, y, z, m) => m.Subordinates, (x, y, z, m, n) => m.Id == n.ManagerId)
                .LeftJoin((x, y, z, m, n) => n.Subordinates, (x, y, z, m, n, o) => n.Id == o.ManagerId)
                .Where((x, y, z, m, n, o) => x.Id == 2)
                .Select(
                    (x, y, z, m, n, o) => new
                    {
                        x.Id,
                        x.Name,
                        Level1SubordinateId = y.Id,
                        Level1SubordinateName = y.Name,
                        Level2SubordinateId = z.Id,
                        Level2SubordinateName = z.Name,
                        Level3SubordinateId = m.Id,
                        Level3SubordinateName = m.Name,
                        Level4SubordinateId = n.Id,
                        Level4SubordinateName = n.Name,
                        Level5SubordinateId = o.Id,
                        Level5SubordinateName = o.Name
                    })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Two_Tables()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            Expression<Func<User, Department>> propertyPath = x => x.Department;
            Expression<Func<User, Department, bool>> condition = (x, y) => x.DepartmentId == y.DepId;

            Expression<Func<User, Department, bool>> predicate = (x, y) => x.Id == 1;
            Expression<Func<User, Department, object>> selector = (x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name };

            await memberDataAccess.QueryOneAsync<Department>((propertyPath, null, condition, JoinType.Inner), predicate, selector: selector);

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Two_Tables_and_Cross_Database_use_QueryObject()
        {
            var advertisementSettingDataAccess = new CapturingDataAccess<AdvertisementSetting>("Advertisement");

            await advertisementSettingDataAccess.InnerJoin(x => x.Owner, (x, y) => x.OwnerId == y.Id)
                .Where((x, y) => x.Type == "1000x90首頁下")
                .Select((x, y) => new { x.Id, OwnerId = y.Id, OwnerName = y.Name })
                .QueryOneAsync();

            await Verify(advertisementSettingDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            Expression<Func<User, Department>> secondPropertyPath = x => x.Department;
            Expression<Func<User, Department, bool>> secondCondition = (x, y) => x.DepartmentId == y.DepId;

            Expression<Func<User, Department, User>> thirdPropertyPath = (x, y) => x.Manager;
            Expression<Func<User, Department, User, bool>> thirdCondition = (x, y, z) => x.ManagerId == z.Id;

            Expression<Func<User, Department, User, bool>> predicate = (x, y, z) => x.Id == 1;
            Expression<Func<User, Department, User, object>> selector = (x, y, z) => new { x.Id, y.DepId, x.Name, ManagerId = z.Id, DepartmentName = y.Name, ManagerName = z.Name };

            await memberDataAccess.QueryOneAsync<Department, User>((secondPropertyPath, null, secondCondition, JoinType.Inner), (thirdPropertyPath, null, thirdCondition, JoinType.Inner), predicate, selector: selector);

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_and_GroupBy_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (a, b) => a.DepartmentId == b.DepId)
                .LeftJoin((c, d) => c.Subordinate, (m, n, o) => o.ManagerId == m.Id)
                .GroupBy(
                    (c, d, e) => new { c.Id, d.Name },
                    g => new User
                         {
                             Id = g.Select((a, b, c) => a.Id),
                             Name = g.Select((m, n, o) => n.Name),
                             SubordinateCount = g.Count()
                         })
                .Where((u1, d1, u2) => u1.Id == 1)
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_and_GroupBy_use_StatisticsModel_and_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<UserStatistics>();

            await memberDataAccess.InnerJoin(x => x.Department, (a, b) => a.DepartmentId == b.DepId)
                .LeftJoin((c, d) => c.Subordinate, (m, n, o) => o.ManagerId == m.Id)
                .GroupBy(
                    (c, d, e) => new { c.Id, c.Name, DepartmentName = d.Name },
                    g => new UserStatistics
                         {
                             Id = g.Select((a, b, c) => a.Id),
                             Name = g.Select((m, n, o) => m.Name),
                             DepartmentName = g.Select((x, y, z) => y.Name),
                             TotalCount = g.Count(),
                             MaxSubordinateId = g.Max((o, p, q) => q.Id),
                             MinSubordinateId = g.Min((us, dep, u) => u.Id),
                             SumSubordinateAge = g.Sum((xx, yy, zz) => zz.Age),
                             AvgSubordinateAge = g.Avg((aa, bb, cc) => cc.Age)
                         })
                .Where((u1, d1, u2) => u1.Id == 1)
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_and_GroupBy_OrderBy_use_StatisticsModel_and_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<UserStatistics>();

            await memberDataAccess.InnerJoin(x => x.Department, (a, b) => a.DepartmentId == b.DepId)
                .LeftJoin((c, d) => c.Subordinate, (m, n, o) => o.ManagerId == m.Id)
                .GroupBy(
                    (c, d, e) => new { c.Id, c.Name, DepartmentName = d.Name },
                    g => new UserStatistics
                         {
                             Id = g.Select((a, b, c) => a.Id),
                             Name = g.Select((m, n, o) => m.Name),
                             DepartmentName = g.Select((x, y, z) => y.Name),
                             SubordinateCount = g.Count((qq, kk, gg) => gg.Name),
                             MaxSubordinateId = g.Max((o, p, q) => q.Id),
                             MinSubordinateId = g.Min((us, dep, u) => u.Id),
                             SumSubordinateAge = g.Sum((xx, yy, zz) => zz.Age),
                             AvgSubordinateAge = g.Avg((aa, bb, cc) => cc.Age)
                         })
                .OrderByDescending((bb, cc, dd) => bb.Id)
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                .Where((x, y, z) => x.Id == 1)
                .Select(
                    (x, y, z) => new
                                 {
                                     x.Id,
                                     y.DepId,
                                     x.Name,
                                     ManagerId = z.Id,
                                     DepartmentName = y.Name,
                                     ManagerName = z.Name
                                 })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                .InnerJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                .Where((x, y, z) => x.Id == 2)
                .Select(
                    (x, y, z) => new
                                 {
                                     x.Id,
                                     x.Name,
                                     Level1SubordinateId = y.Id,
                                     Level1SubordinateName = y.Name,
                                     Level2SubordinateId = z.Id,
                                     Level2SubordinateName = z.Name
                                 })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Five_Tables_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(a => a.Department, (a, b) => a.DepartmentId == b.DepId)
                .InnerJoin((a, b) => a.Manager, (a, b, c) => a.ManagerId == c.Id)
                .InnerJoin((a, b, c) => c.Department, (a, b, c, d) => c.DepartmentId == d.DepId)
                .InnerJoin((a, b, c, d) => c.Manager, (a, b, c, d, e) => c.ManagerId == e.Id)
                .Where((a, b, c, d, e) => a.Id == 1)
                .Select(
                    (a, b, c, d, e) => new
                                       {
                                           a.Id,
                                           a.Name,
                                           DepartmentId = b.DepId,
                                           DepartmentName = b.Name,
                                           ManagerId = c.Id,
                                           ManagerName = c.Name,
                                           ManagerDepartmentId = d.DepId,
                                           ManagerDepartmentName = d.Name,
                                           ManagerOfManagerId = e.Id,
                                           ManagerOfManagerName = e.Name
                                       })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Six_Tables_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(a => a.Department, (a, b) => a.DepartmentId == b.DepId)
                .InnerJoin((a, b) => a.Manager, (a, b, c) => a.ManagerId == c.Id)
                .InnerJoin((a, b, c) => c.Department, (a, b, c, d) => c.DepartmentId == d.DepId)
                .InnerJoin((a, b, c, d) => c.Manager, (a, b, c, d, e) => c.ManagerId == e.Id)
                .InnerJoin((a, b, c, d, e) => e.Department, (a, b, c, d, e, f) => e.DepartmentId == f.DepId)
                .Where((a, b, c, d, e, f) => a.Id == 1)
                .Select(
                    (a, b, c, d, e, f) => new
                                          {
                                              a.Id,
                                              a.Name,
                                              DepartmentId = b.DepId,
                                              DepartmentName = b.Name,
                                              ManagerId = c.Id,
                                              ManagerName = c.Name,
                                              ManagerDepartmentId = d.DepId,
                                              ManagerDepartmentName = d.Name,
                                              ManagerOfManagerId = e.Id,
                                              ManagerOfManagerName = e.Name,
                                              ManagerOfManagerDepartmentId = f.DepId,
                                              ManagerOfManagerDepartmentName = f.Name
                                          })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Seven_Tables_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(a => a.Department, (a, b) => a.DepartmentId == b.DepId)
                .InnerJoin((a, b) => a.Manager, (a, b, c) => a.ManagerId == c.Id)
                .InnerJoin((a, b, c) => c.Department, (a, b, c, d) => c.DepartmentId == d.DepId)
                .InnerJoin((a, b, c, d) => c.Manager, (a, b, c, d, e) => c.ManagerId == e.Id)
                .InnerJoin((a, b, c, d, e) => e.Department, (a, b, c, d, e, f) => e.DepartmentId == f.DepId)
                .InnerJoin((a, b, c, d, e, f) => e.Manager, (a, b, c, d, e, f, g) => e.ManagerId == g.Id)
                .Where((a, b, c, d, e, f, g) => a.Id == 1)
                .Select(
                    (a, b, c, d, e, f, g) => new
                                             {
                                                 a.Id,
                                                 a.Name,
                                                 DepartmentId = b.DepId,
                                                 DepartmentName = b.Name,
                                                 ManagerId = c.Id,
                                                 ManagerName = c.Name,
                                                 ManagerDepartmentId = d.DepId,
                                                 ManagerDepartmentName = d.Name,
                                                 ManagerOfManagerId = e.Id,
                                                 ManagerOfManagerName = e.Name,
                                                 ManagerOfManagerDepartmentId = f.DepId,
                                                 ManagerOfManagerDepartmentName = f.Name,
                                                 ManagerOfManagerOfManagerId = g.Id,
                                                 ManagerOfManagerOfManagerName = g.Name
                                             })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Cross_InnerJoin_Four_Tables_and_Different_Lambda_ParameterNames_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(a => a.Manager, (a, b) => a.ManagerId == b.Id)
                .InnerJoin((c, d) => c.Department, (c, d, e) => c.DepartmentId == e.DepId)
                .InnerJoin((f, g, h) => g.Department, (f, g, h, j) => g.DepartmentId == j.DepId)
                .Where((k, m, n, o) => k.Id == 1)
                .Select(
                    (x, y, z, t) => new
                                    {
                                        x.Id,
                                        x.Name,
                                        ManagerId = y.Id,
                                        ManagerName = y.Name,
                                        DepartmentId = z.DepId,
                                        DepartmentName = z.Name,
                                        ManagerDepartmentId = t.DepId,
                                        ManagerDepartmentName = t.Name
                                    })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Cross_InnerJoin_Four_Tables_and_Mass_Column_Sequence_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id)
                .InnerJoin((x, y) => x.Department, (x, y, z) => x.DepartmentId == z.DepId)
                .InnerJoin((x, y, z) => y.Department, (x, y, z, t) => y.DepartmentId == t.DepId)
                .Where((x, y, z, t) => x.Id == 1)
                .Select(
                    (x, y, z, t) => new
                                    {
                                        x.Name,
                                        ManagerDepartmentName = t.Name,
                                        ManagerName = y.Name,
                                        DepartmentName = z.Name,
                                        ManagerDepartmentId = t.DepId,
                                        DepartmentId = z.DepId,
                                        ManagerId = y.Id,
                                        x.Id
                                    })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Nested_InnerJoin_Three_Tables_and_Different_Lambda_ParameterNames()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            Expression<Func<User, User>> secondPropertyPath = a => a.Manager;
            Expression<Func<User, User, bool>> secondCondition = (x, y) => x.ManagerId == y.Id;

            Expression<Func<User, User, Department>> thirdPropertyPath = (c, d) => d.Department;
            Expression<Func<User, User, Department, bool>> thirdCondition = (m, n, o) => n.DepartmentId == o.DepId;

            Expression<Func<User, User, Department, bool>> predicate = (o, n, m) => o.Id == 1;
            Expression<Func<User, User, Department, object>> selector = (c, b, a) => new { c.Id, ManagerId = b.Id, c.Name, a.DepId, ManagerName = b.Name, DepartmentName = a.Name };

            await memberDataAccess.QueryOneAsync<User, Department>((secondPropertyPath, null, secondCondition, JoinType.Inner), (thirdPropertyPath, null, thirdCondition, JoinType.Inner), predicate, selector: selector);

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Nested_InnerJoin_Three_Tables_and_Different_Lambda_ParameterNames_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(a => a.Manager, (x, y) => x.ManagerId == y.Id)
                .InnerJoin((c, d) => d.Department, (m, n, o) => n.DepartmentId == o.DepId)
                .Where((o, n, m) => o.Id == 1)
                .Select(
                    (c, b, a) => new
                                 {
                                     c.Id,
                                     ManagerId = b.Id,
                                     c.Name,
                                     a.DepId,
                                     ManagerName = b.Name,
                                     DepartmentName = a.Name
                                 })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .Where((x, y) => x.Id == 1)
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Self_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                .Where((x, y) => x.Id == 1)
                .Select((x, y) => new { x.Id, SelfId = y.Id, x.Name, SelfName = y.Name })
                .QueryOneAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }
    }
}
