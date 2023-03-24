using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests
{
    public partial class SqlServerDataAccessTest
    {
        [TestMethod]
        public async Task Test_QueryOneAsync_use_String_Comparison()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.Where(x => x.Name.CompareTo("J") >= 0).Select(x => new { x.Id }).QueryAsync();

            result.Count.Should().BeGreaterOrEqualTo(2);
            result.Select(x => x.Id).Should().Contain(new[] { 1, 3 });
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Two_Tables_OneToMany_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Departments, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryOneAsync();

            result.Name.Should().Be("Johnny");
            result.Departments[0].DepId.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Three_Tables_OneToMany_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            result.Name.Should().Be("Johnny");
            result.Departments.Count.Should().Be(1);
            result.Departments[0].DepId.Should().Be(3);
            result.Subordinates.Count.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Four_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            result.Subordinates.Count.Should().Be(1);
            result.Subordinates[0].Id.Should().Be(1);
            result.Subordinates[0].Subordinates.Count.Should().Be(3);
            result.Subordinates[0].Subordinates.Any(x => x.Id == 2).Should().BeTrue();
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates.Count.Should().Be(1);
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Five_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            result.Subordinates.Count.Should().Be(1);
            result.Subordinates[0].Id.Should().Be(1);
            result.Subordinates[0].Subordinates.Count.Should().Be(3);
            result.Subordinates[0].Subordinates.Any(x => x.Id == 2).Should().BeTrue();
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates.Count.Should().Be(1);
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Count.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_LeftJoin_Six_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            result.Subordinates.Count.Should().Be(1);
            result.Subordinates[0].Id.Should().Be(1);
            result.Subordinates[0].Subordinates.Count.Should().Be(3);
            result.Subordinates[0].Subordinates.Any(x => x.Id == 2).Should().BeTrue();
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates.Count.Should().Be(1);
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Count.Should().Be(3);
            result.Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Two_Tables()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            Expression<Func<User, Department>> propertyPath = x => x.Department;
            Expression<Func<User, Department, bool>> condition = (x, y) => x.DepartmentId == y.DepId;

            Expression<Func<User, Department, bool>> predicate = (x, y) => x.Id == 1;
            Expression<Func<User, Department, object>> selector = (x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name };

            var result = await memberDataAccess.QueryOneAsync<Department>((propertyPath, null, condition, JoinType.Inner), predicate, selector: selector);

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
        }

        [TestMethod]
        public void Test_QueryOneAsync_with_InnerJoin_Two_Tables_will_Throw_Different_Database_Server_Exception()
        {
            var passDataAccess = DataAccessFactory.Create<Pass>();

            passDataAccess.Invoking(
                    dataAccess =>
                        {
                            passDataAccess.InnerJoin(x => x.Product, (x, y) => x.ProductId == y.Id)
                                .Where((x, y) => x.MemberNo == 0000)
                                .Select((x, y) => new { x.MemberNo, y.Name })
                                .QueryOneAsync().Wait();
                        })
                .Should()
                .Throw<ArgumentException>()
                .WithMessage("Table is not in the same database server.");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Two_Tables_and_Cross_Database_use_QueryObject()
        {
            var advertisementSettingDataAccess = DataAccessFactory.Create<AdvertisementSetting>(null, "Advertisement");

            var result = await advertisementSettingDataAccess.InnerJoin(x => x.Owner, (x, y) => x.OwnerId == y.Id)
                             .Where((x, y) => x.Type == "1000x90首頁下")
                             .Select((x, y) => new { x.Id, OwnerId = y.Id, OwnerName = y.Name })
                             .QueryOneAsync();

            result.Id.Should().Be(Guid.Parse("df31efe5-b78f-4b4b-954a-0078328e34d2"));
            result.Owner.Id.Should().Be(1);
            result.Owner.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            Expression<Func<User, Department>> secondPropertyPath = x => x.Department;
            Expression<Func<User, Department, bool>> secondCondition = (x, y) => x.DepartmentId == y.DepId;

            Expression<Func<User, Department, User>> thirdPropertyPath = (x, y) => x.Manager;
            Expression<Func<User, Department, User, bool>> thirdCondition = (x, y, z) => x.ManagerId == z.Id;

            Expression<Func<User, Department, User, bool>> predicate = (x, y, z) => x.Id == 1;
            Expression<Func<User, Department, User, object>> selector = (x, y, z) => new { x.Id, y.DepId, x.Name, ManagerId = z.Id, DepartmentName = y.Name, ManagerName = z.Name };

            var result = await memberDataAccess.QueryOneAsync<Department, User>((secondPropertyPath, null, secondCondition, JoinType.Inner), (thirdPropertyPath, null, thirdCondition, JoinType.Inner), predicate, selector: selector);

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_and_GroupBy_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (a, b) => a.DepartmentId == b.DepId)
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

            result.Id.Should().Be(1);
            result.Name.Should().Be("董事長室");
            result.SubordinateCount.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_and_GroupBy_use_StatisticsModel_and_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<UserStatistics>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (a, b) => a.DepartmentId == b.DepId)
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

            result.Id.Should().Be(1);
            result.Name.Should().Be("Johnny");
            result.DepartmentName.Should().Be("董事長室");
            result.TotalCount.Should().Be(3);
            result.MaxSubordinateId.Should().Be(4);
            result.MinSubordinateId.Should().Be(2);
            result.SumSubordinateAge.Should().Be(109);
            Math.Round(result.AvgSubordinateAge, 2).Should().Be(36.33m);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_and_GroupBy_OrderBy_use_StatisticsModel_and_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<UserStatistics>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (a, b) => a.DepartmentId == b.DepId)
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

            // 因為有 InnerJoin 部門，有一個 Member 沒有部門。
            result.Count.Should().Be(3);

            var firstResult = result.First();

            firstResult.Name.Should().Be("ThreeM");
            firstResult.DepartmentName.Should().Be("行銷部");
            firstResult.SubordinateCount.Should().Be(0);
            firstResult.MaxSubordinateId.Should().Be(0);
            firstResult.MinSubordinateId.Should().Be(0);
            firstResult.SumSubordinateAge.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
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

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Three_Tables_OneToMany_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            result.Subordinates.Count.Should().Be(1);
            result.Subordinates[0].Id.Should().Be(1);
            result.Subordinates[0].Subordinates.Count.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Five_Tables_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(a => a.Department, (a, b) => a.DepartmentId == b.DepId)
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

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
            result.Manager.Department.DepId.Should().Be(2);
            result.Manager.Department.Name.Should().Be("業務部");
            result.Manager.Manager.Id.Should().Be(1);
            result.Manager.Manager.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Six_Tables_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(a => a.Department, (a, b) => a.DepartmentId == b.DepId)
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

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
            result.Manager.Department.DepId.Should().Be(2);
            result.Manager.Department.Name.Should().Be("業務部");
            result.Manager.Manager.Id.Should().Be(1);
            result.Manager.Manager.Name.Should().Be("Johnny");
            result.Manager.Manager.Department.DepId.Should().Be(3);
            result.Manager.Manager.Department.Name.Should().Be("董事長室");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Seven_Tables_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(a => a.Department, (a, b) => a.DepartmentId == b.DepId)
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

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
            result.Manager.Department.DepId.Should().Be(2);
            result.Manager.Department.Name.Should().Be("業務部");
            result.Manager.Manager.Id.Should().Be(1);
            result.Manager.Manager.Name.Should().Be("Johnny");
            result.Manager.Manager.Department.DepId.Should().Be(3);
            result.Manager.Manager.Department.Name.Should().Be("董事長室");
            result.Manager.Manager.Manager.Id.Should().Be(2);
            result.Manager.Manager.Manager.Name.Should().Be("Amy");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Cross_InnerJoin_Four_Tables_and_Different_Lambda_ParameterNames_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(a => a.Manager, (a, b) => a.ManagerId == b.Id)
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

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
            result.Manager.Department.DepId.Should().Be(2);
            result.Manager.Department.Name.Should().Be("業務部");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Cross_InnerJoin_Four_Tables_and_Mass_Column_Sequence_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id)
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

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
            result.Department.Name.Should().Be("董事長室");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
            result.Manager.Department.DepId.Should().Be(2);
            result.Manager.Department.Name.Should().Be("業務部");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Nested_InnerJoin_Three_Tables_and_Different_Lambda_ParameterNames()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            Expression<Func<User, User>> secondPropertyPath = a => a.Manager;
            Expression<Func<User, User, bool>> secondCondition = (x, y) => x.ManagerId == y.Id;

            Expression<Func<User, User, Department>> thirdPropertyPath = (c, d) => d.Department;
            Expression<Func<User, User, Department, bool>> thirdCondition = (m, n, o) => n.DepartmentId == o.DepId;

            Expression<Func<User, User, Department, bool>> predicate = (o, n, m) => o.Id == 1;
            Expression<Func<User, User, Department, object>> selector = (c, b, a) => new { c.Id, ManagerId = b.Id, c.Name, a.DepId, ManagerName = b.Name, DepartmentName = a.Name };

            var result = await memberDataAccess.QueryOneAsync<User, Department>((secondPropertyPath, null, secondCondition, JoinType.Inner), (thirdPropertyPath, null, thirdCondition, JoinType.Inner), predicate, selector: selector);

            result.Name.Should().Be("Johnny");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
            result.Manager.Department.DepId.Should().Be(2);
            result.Manager.Department.Name.Should().Be("業務部");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Nested_InnerJoin_Three_Tables_and_Different_Lambda_ParameterNames_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(a => a.Manager, (x, y) => x.ManagerId == y.Id)
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

            result.Name.Should().Be("Johnny");
            result.Manager.Id.Should().Be(2);
            result.Manager.Name.Should().Be("Amy");
            result.Manager.Department.DepId.Should().Be(2);
            result.Manager.Department.Name.Should().Be("業務部");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => x.Id == 1)
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryOneAsync();

            result.Name.Should().Be("Johnny");
            result.Department.DepId.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_InnerJoin_Self_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Self, (x, y) => x.Id == y.Id)
                             .Where((x, y) => x.Id == 1)
                             .Select((x, y) => new { x.Id, SelfId = y.Id, x.Name, SelfName = y.Name })
                             .QueryOneAsync();

            result.Id.Should().Be(1);
            result.Name.Should().Be("Johnny");
            result.Self.Id.Should().Be(1);
            result.Self.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public void Test_QueryOneAsync_with_InnerJoin_Two_Tables_only_Left_will_Throw_ArgumentException()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            memberDataAccess
                .Invoking(
                    async dataAccess => await dataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                                            .Where((x, y) => x.Id == 1)
                                            .Select((x, y) => new { x.Id, x.Name })
                                            .QueryOneAsync())
                .Should()
                .Throw<InvalidOperationException>()
                .WithMessage("Selected columns must cover all joined tables.");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var club = await clubDataAccess.QueryOneAsync(x => x.Id == 25, null, x => new { x.Name });

            club.Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Selector()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var club = await clubDataAccess.QueryOneAsync(x => x.Id == 25, selector: x => new { x.Name });

            club.Id.Should().Be(0);
            club.Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Selector_use_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var club = await clubDataAccess.Where(x => x.Id == 25).Select(x => new { x.Name }).QueryOneAsync();

            club.Id.Should().Be(0);
            club.Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_use_Pagination_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var club = await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(3).Take(1).QueryOneAsync();

            club.Id.Should().Be(12);
            club.Name.Should().Be("黃亮香");
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_AS_Keyword_Alias()
        {
            var advertisementSettingDataAccess = DataAccessFactory.Create<AdvertisementSetting>(null, "Advertisement");

            var result = await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            result.Id.Should().Be(Guid.Parse("df31efe5-b78f-4b4b-954a-0078328e34d2"));
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_assign_ConnectionString_Name()
        {
            var advertisementSettingDataAccess = DataAccessFactory.Create<AdvertisementSetting>(null, "Advertisement2");

            var result = await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            result.Id.Should().Be(Guid.Parse("df31efe5-b78f-4b4b-954a-0078328e34d2"));
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_Multiple_ConnectionStringAttributes()
        {
            var advertisementSettingDataAccess = DataAccessFactory.Create<DerivedAdvertisementSetting>(null, "Advertisement");

            var result = await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            result.Id.Should().Be(Guid.Parse("df31efe5-b78f-4b4b-954a-0078328e34d2"));
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_with_assign_ConnectionString_Name_from_DerivedClass()
        {
            var advertisementSettingDataAccess = DataAccessFactory.Create<DerivedAdvertisementSetting>(null, "Advertisement3");

            var result = await advertisementSettingDataAccess.Where(x => x.Type == "1000x90首頁下").Select(x => new { x.Id }).QueryOneAsync();

            result.Id.Should().Be(Guid.Parse("df31efe5-b78f-4b4b-954a-0078328e34d2"));
        }

        [TestMethod]
        public async Task Test_QueryOneAsync_use_this_Keyword()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var club = await clubDataAccess.Where(x => x.Id == this.Club.Id).Select(x => new { x.Name }).QueryOneAsync();

            club.Id.Should().Be(0);
            club.Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public void Test_QueryOneAsync_without_ConnectionString_Name_in_Multiple_ConnectionStringAttribute_will_Throw_ArgumentException()
        {
            DataAccessFactory.Invoking(factory => factory.Create<AdvertisementSetting>())
                .Should()
                .Throw<ArgumentException>()
                .WithMessage("Must indicate connection string.");
        }

        [TestMethod]
        public void Test_QueryOneAsync_without_ConnectionString_will_Throw_ArgumentException()
        {
            DataAccessFactory.Invoking(factory => factory.Create<AnotherMember>())
                .Should()
                .Throw<ArgumentException>()
                .WithMessage("Must add connection string.");
        }

        [TestMethod]
        public void Test_QueryOneAsync_with_Non_Existent_Prop_will_Throw_SqlException()
        {
            IDictionary<string, object> param = null;

            DataAccessFactory.OnDbError += (ex, sql, args) =>
                {
                    param = (IDictionary<string, object>)args;

                    param.Add("Factory OnDbError", string.Empty);
                };

            DataAccessFactory.Invoking(
                    factory =>
                        {
                            var userDataAccess = factory.Create<User>();

                            userDataAccess.OnDbError += (ex, sql, args) =>
                                {
                                    param = (IDictionary<string, object>)args;

                                    param.Add("DataAccess OnDbError", string.Empty);
                                };

                            var user = userDataAccess.Where(x => x.Id == 1)
                                .Select(x => new { x.Id, x.NonExistentProp })
                                .QueryOneAsync()
                                .Result;
                        })
                .Should()
                .Throw<SqlException>()
                .WithMessage("Invalid column name 'NonExistentProp'.");

            param.Should().ContainKey("Factory OnDbError");
            param.Should().ContainKey("DataAccess OnDbError");
        }
    }
}