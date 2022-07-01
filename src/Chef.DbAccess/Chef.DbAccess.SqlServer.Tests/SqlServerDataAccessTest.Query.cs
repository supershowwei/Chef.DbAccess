using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests
{
    public partial class SqlServerDataAccessTest
    {
        [TestMethod]
        public void Test_QueryAsync_use_Null_Selector_will_Throw_ArgumentException()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            clubDataAccess.Invoking(async dataAccess => await Extension.QueryOneAsync<Club>(dataAccess.Where(x => x.Id == 35)))
                .Should()
                .Throw<ArgumentException>()
                .WithMessage("Must be at least one column selected.");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_GroupBY_and_OrderBy()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.GroupBy(x => new { x.Id }, g => new Club { Id = g.Select(x => x.Id) })
                            .OrderBy(x => x.Id)
                            .QueryAsync();

            clubs.First().Id.Should().Be(9);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Department.DepId.Should().Be(3);
            result[1].Name.Should().Be("Amy");
            result[1].Department.DepId.Should().Be(2);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Departments, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Departments[0].DepId.Should().Be(3);
            result[1].Name.Should().Be("Amy");
            result[1].Departments[0].DepId.Should().Be(2);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_with_Distinct_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(c => c.Manager, (n, o) => n.ManagerId == o.Id)
                             .Where((c, e) => e.Id == 1)
                             .Distinct((x, y) => new { x.ManagerId, y.Id, y.Name })
                             .QueryAsync();

            result.Count.Should().Be(1);
            result[0].Manager.Id.Should().Be(1);
            result[0].Manager.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_with_Deduplicate_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(c => c.Subordinates, (n, o) => n.Id == o.ManagerId)
                             .Where((c, e) => c.Id == 1)
                             .Select((x, y) => new { x.Id, x.Name, SubordinateId = y.Id, SubordinateName = y.Name })
                             .QueryAsync();

            result.Count.Should().Be(1);
            result[0].Id.Should().Be(1);
            result[0].Name.Should().Be("Johnny");
            result[0].Subordinates.Count.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_Constant_in_On_Condition_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId && (x.DepartmentId == 2 || x.DepartmentId == 3))
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Department.DepId.Should().Be(3);
            result[1].Name.Should().Be("Amy");
            result[1].Department.DepId.Should().Be(2);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_Property_in_On_Condition_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var depId = 2;
            var dep = new { Id = 3 };

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId && (x.DepartmentId == depId || x.DepartmentId == dep.Id))
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Department.DepId.Should().Be(3);
            result[1].Name.Should().Be("Amy");
            result[1].Department.DepId.Should().Be(2);
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_OrderBy_with_InnerJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .OrderBy((x, y) => y.DepId)
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Amy");
            result[0].Department.DepId.Should().Be(2);
            result[1].Name.Should().Be("Johnny");
            result[1].Department.DepId.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_And_with_InnerJoin_Two_Tables()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .And((x, y) => y.DepId == 3)
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            result.Count.Should().Be(1);
            result[0].Name.Should().Be("Johnny");
            result[0].Department.DepId.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Nested_InnerJoin_Three_Tables()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id)
                             .InnerJoin((x, y) => y.Department, (x, y, z) => y.DepartmentId == z.DepId)
                             .Where((x, y, z) => new[] { 1, 3 }.Contains(x.Id))
                             .Select(
                                 (x, y, z) => new
                                              {
                                                  x.Id,
                                                  ManagerId = y.Id,
                                                  x.Name,
                                                  z.DepId,
                                                  ManagerName = y.Name,
                                                  DepartmentName = z.Name
                                              })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Manager.Id.Should().Be(2);
            result[0].Manager.Name.Should().Be("Amy");
            result[0].Manager.Department.DepId.Should().Be(2);
            result[0].Manager.Department.Name.Should().Be("業務部");
            result[1].Name.Should().Be("ThreeM");
            result[1].Manager.Id.Should().Be(1);
            result[1].Manager.Name.Should().Be("Johnny");
            result[1].Manager.Department.DepId.Should().Be(3);
            result[1].Manager.Department.Name.Should().Be("董事長室");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Three_Tables_OneToMany()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                             .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                             .Where((x, y, z) => new[] { 2, 4 }.Contains(x.Id))
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
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Subordinates.Count.Should().Be(1);
            result[0].Subordinates[0].Id.Should().Be(1);
            result[0].Subordinates[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates.Should().BeEmpty();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Four_Tables_OneToMany()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                             .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                             .LeftJoin((x, y, z) => z.Subordinates, (x, y, z, m) => z.Id == m.ManagerId)
                             .Where((x, y, z, m) => new[] { 1, 2 }.Contains(x.Id))
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
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates[0].Subordinates.Any(x => x.Id == 2).Should().BeTrue();
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_OneToMany()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                             .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                             .LeftJoin((x, y, z) => z.Subordinates, (x, y, z, m) => z.Id == m.ManagerId)
                             .LeftJoin((x, y, z, m) => m.Subordinates, (x, y, z, m, n) => m.Id == n.ManagerId)
                             .Where((x, y, z, m, n) => new[] { 1, 2 }.Contains(x.Id))
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
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates[0].Subordinates.Any(x => x.Id == 2).Should().BeTrue();
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Count.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Six_Tables_OneToMany()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                             .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                             .LeftJoin((x, y, z) => z.Subordinates, (x, y, z, m) => z.Id == m.ManagerId)
                             .LeftJoin((x, y, z, m) => m.Subordinates, (x, y, z, m, n) => m.Id == n.ManagerId)
                             .LeftJoin((x, y, z, m, n) => n.Subordinates, (x, y, z, m, n, o) => n.Id == o.ManagerId)
                             .Where((x, y, z, m, n, o) => new[] { 1, 2 }.Contains(x.Id))
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
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates[0].Subordinates.Any(x => x.Id == 2).Should().BeTrue();
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Six_Tables_OneToMany_and_Null_OneToOne()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                             .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                             .LeftJoin((x, y, z) => z.Department, (x, y, z, m) => z.DepartmentId == m.DepId)
                             .Where((x, y, z, m) => new[] { 1, 2 }.Contains(x.Id))
                             .Select(
                                 (x, y, z, m) => new
                                                 {
                                                     x.Id,
                                                     x.Name,
                                                     Level1SubordinateId = y.Id,
                                                     Level1SubordinateName = y.Name,
                                                     Level2SubordinateId = z.Id,
                                                     Level2SubordinateName = z.Name,
                                                     DepartmentId = m.DepId,
                                                     DepartmentName = m.Name
                                                 })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Subordinates.Count.Should().Be(3);
            result[0].Subordinates.Where(x => x.Id != 2).All(x => x.Subordinates.Count == 0).Should().BeTrue();
            result[1].Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Count.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Seven_Tables_OneToMany()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
                             .LeftJoin((x, y) => y.Subordinates, (x, y, z) => y.Id == z.ManagerId)
                             .LeftJoin((x, y, z) => z.Subordinates, (x, y, z, m) => z.Id == m.ManagerId)
                             .LeftJoin((x, y, z, m) => m.Subordinates, (x, y, z, m, n) => m.Id == n.ManagerId)
                             .LeftJoin((x, y, z, m, n) => n.Subordinates, (x, y, z, m, n, o) => n.Id == o.ManagerId)
                             .LeftJoin((x, y, z, m, n, o) => o.Subordinates, (x, y, z, m, n, o, p) => o.Id == p.ManagerId)
                             .Where((x, y, z, m, n, o, p) => new[] { 1, 2 }.Contains(x.Id))
                             .Select(
                                 (x, y, z, m, n, o, p) => new
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
                                                              Level5SubordinateName = o.Name,
                                                              Level6SubordinateId = p.Id,
                                                              Level6SubordinateName = p.Name
                                                          })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates[0].Subordinates.Any(x => x.Id == 2).Should().BeTrue();
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates.Count.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Count.Should().Be(3);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Id.Should().Be(1);
            result[1].Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Single(x => x.Id == 2).Subordinates[0].Subordinates.Count.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 4 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Department.DepId.Should().Be(3);
            result[1].Name.Should().Be("Flosser");
            result[1].Department.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(v => v.Department, (v, w) => v.DepartmentId == w.DepId)
                             .LeftJoin((v, w) => v.Self, (v, w, x) => v.Id == x.Id)
                             .LeftJoin((v, w, x) => x.Department, (v, w, x, y) => x.DepartmentId == y.DepId)
                             .InnerJoin((v, w, x, y) => x.Manager, (v, w, x, y, z) => x.ManagerId == z.Id)
                             .Where((v, w, x, y, z) => new[] { 1, 4 }.Contains(v.Id))
                             .Select(
                                 (v, w, x, y, z) => new
                                                    {
                                                        v.Id,
                                                        v.Name,
                                                        DepartmentId = w.DepId,
                                                        DepartmentName = w.Name,
                                                        ManagerId = x.Id,
                                                        ManagerName = x.Name,
                                                        ManagerDepartmentId = y.DepId,
                                                        ManagerDepartmentName = y.Name,
                                                        ManagerOfManagerId = z.Id,
                                                        ManagerOfManagerName = z.Name
                                                    })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Department.DepId.Should().Be(3);
            result[0].Department.Name.Should().Be("董事長室");
            result[0].Self.Id.Should().Be(1);
            result[0].Self.Name.Should().Be("Johnny");
            result[0].Self.Department.DepId.Should().Be(3);
            result[0].Self.Department.Name.Should().Be("董事長室");
            result[0].Self.Manager.Id.Should().Be(2);
            result[0].Self.Manager.Name.Should().Be("Amy");

            result[1].Name.Should().Be("Flosser");
            result[1].Department.Should().BeNull();
            result[1].Self.Id.Should().Be(4);
            result[1].Self.Name.Should().Be("Flosser");
            result[1].Self.Department.Should().BeNull();
            result[1].Self.Manager.Id.Should().Be(1);
            result[1].Self.Manager.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_and_Or_Condition_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(v => v.Department, (v, w) => v.DepartmentId == w.DepId)
                             .LeftJoin((v, w) => v.Self, (v, w, x) => v.Id == x.Id)
                             .LeftJoin((v, w, x) => x.Department, (v, w, x, y) => x.DepartmentId == y.DepId)
                             .InnerJoin((v, w, x, y) => x.Manager, (v, w, x, y, z) => x.ManagerId == z.Id)
                             .Where((v, w, x, y, z) => v.Id == 1)
                             .Or((v, w, x, y, z) => v.Id == 4)
                             .Select(
                                 (v, w, x, y, z) => new
                                                    {
                                                        v.Id,
                                                        v.Name,
                                                        DepartmentId = w.DepId,
                                                        DepartmentName = w.Name,
                                                        ManagerId = x.Id,
                                                        ManagerName = x.Name,
                                                        ManagerDepartmentId = y.DepId,
                                                        ManagerDepartmentName = y.Name,
                                                        ManagerOfManagerId = z.Id,
                                                        ManagerOfManagerName = z.Name
                                                    })
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Johnny");
            result[0].Department.DepId.Should().Be(3);
            result[0].Department.Name.Should().Be("董事長室");
            result[0].Self.Id.Should().Be(1);
            result[0].Self.Name.Should().Be("Johnny");
            result[0].Self.Department.DepId.Should().Be(3);
            result[0].Self.Department.Name.Should().Be("董事長室");
            result[0].Self.Manager.Id.Should().Be(2);
            result[0].Self.Manager.Name.Should().Be("Amy");

            result[1].Name.Should().Be("Flosser");
            result[1].Department.Should().BeNull();
            result[1].Self.Id.Should().Be(4);
            result[1].Self.Name.Should().Be("Flosser");
            result[1].Self.Department.Should().BeNull();
            result[1].Self.Manager.Id.Should().Be(1);
            result[1].Self.Manager.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_and_OrderByDescending_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(v => v.Department, (v, w) => v.DepartmentId == w.DepId)
                             .LeftJoin((v, w) => v.Self, (v, w, x) => v.Id == x.Id)
                             .LeftJoin((v, w, x) => x.Department, (v, w, x, y) => x.DepartmentId == y.DepId)
                             .InnerJoin((v, w, x, y) => x.Manager, (v, w, x, y, z) => x.ManagerId == z.Id)
                             .Where((v, w, x, y, z) => new[] { 1, 4 }.Contains(v.Id))
                             .Select(
                                 (v, w, x, y, z) => new
                                                    {
                                                        v.Id,
                                                        v.Name,
                                                        DepartmentId = w.DepId,
                                                        DepartmentName = w.Name,
                                                        ManagerId = x.Id,
                                                        ManagerName = x.Name,
                                                        ManagerDepartmentId = y.DepId,
                                                        ManagerDepartmentName = y.Name,
                                                        ManagerOfManagerId = z.Id,
                                                        ManagerOfManagerName = z.Name
                                                    })
                             .OrderByDescending((v, w, x, y, z) => v.Id)
                             .QueryAsync();

            result.Count.Should().Be(2);
            result[0].Name.Should().Be("Flosser");
            result[0].Department.Should().BeNull();
            result[0].Self.Id.Should().Be(4);
            result[0].Self.Name.Should().Be("Flosser");
            result[0].Self.Department.Should().BeNull();
            result[0].Self.Manager.Id.Should().Be(1);
            result[0].Self.Manager.Name.Should().Be("Johnny");

            result[1].Name.Should().Be("Johnny");
            result[1].Department.DepId.Should().Be(3);
            result[1].Department.Name.Should().Be("董事長室");
            result[1].Self.Id.Should().Be(1);
            result[1].Self.Name.Should().Be("Johnny");
            result[1].Self.Department.DepId.Should().Be(3);
            result[1].Self.Department.Name.Should().Be("董事長室");
            result[1].Self.Manager.Id.Should().Be(2);
            result[1].Self.Manager.Name.Should().Be("Amy");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Null()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.Intro == null).Select(x => new { x.Id }).QueryAsync();

            clubs.Count.Should().BeGreaterOrEqualTo(5);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.QueryAsync(x => new[] { 17, 25 }.Contains(x.Id), selector: x => new { x.Name });

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(0);
            clubs[1].Id.Should().Be(0);
            clubs[0].Name.Should().Be("吳淑娟");
            clubs[1].Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_List_Contains_with_And()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var ids = new List<int> { 17, 25 };
            var active = true;

            var clubs = await clubDataAccess.Where(x => ids.Contains(x.Id))
                            .And(x => x.IsActive == active)
                            .Select(x => new { x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(0);
            clubs[1].Id.Should().Be(0);
            clubs[0].Name.Should().Be("吳淑娟");
            clubs[1].Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_use_Pagination_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(3).Take(2).QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(12);
            clubs[1].Id.Should().Be(15);
            clubs[0].Name.Should().Be("黃亮香");
            clubs[1].Name.Should().Contain("歐陽邦瑋");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id)).Select(x => new { x.Name }).QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(0);
            clubs[1].Id.Should().Be(0);
            clubs[0].Name.Should().Be("吳淑娟");
            clubs[1].Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_use_Pagination_only_Skip_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(26).QueryAsync();

            clubs.Count.Should().BeGreaterOrEqualTo(2);
            clubs.Select(x => x.Id).Should().Contain(new[] { 38, 39 });
            clubs.Select(x => x.Name).Should().Contain(new[] { "謝秋齊", "王真希" });
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_has_DateTime_Now()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.RunningTime < DateTime.Now).Select(x => new { x.Id, x.Name }).QueryAsync();

            clubs.Count.Should().Be(1);
            clubs[0].Id.Should().Be(39);
            clubs[0].Name.Should().Be("王真希");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_And()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.RunningTime < DateTime.Now)
                            .And(y => y.IsActive == false)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_Or()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.RunningTime > DateTime.Now)
                            .Or(y => y.Name == "王真希")
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(1);
            clubs[0].Id.Should().Be(39);
            clubs[0].Name.Should().Be("王真希");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_OrderByDescending()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                            .OrderByDescending(x => x.Id)
                            .Select(x => new { x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(0);
            clubs[1].Id.Should().Be(0);
            clubs[0].Name.Should().Be("鄧偉成");
            clubs[1].Name.Should().Be("吳淑娟");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_Take_without_Ordering()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                            .Select(x => new { x.Name })
                            .Take(1)
                            .QueryAsync();

            clubs.Count.Should().Be(1);
            clubs[0].Id.Should().Be(0);
            clubs[0].Name.Should().Be("吳淑娟");
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_OrderByDescending_and_Take()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                            .OrderByDescending(x => x.Id)
                            .Select(x => new { x.Name })
                            .Take(1)
                            .QueryAsync();

            clubs.Count.Should().Be(1);
            clubs[0].Id.Should().Be(0);
            clubs[0].Name.Should().Be("鄧偉成");
        }

        [TestMethod]
        public async Task Test_QueryAllAsync_with_Selector_use_QueryObject_and_OrderByDescending_and_ThenBy_Take()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.OrderByDescending(x => x.IsActive)
                            .ThenBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .Take(1)
                            .QueryAsync();

            clubs.Count.Should().Be(1);
            clubs[0].Id.Should().Be(9);
            clubs[0].Name.Should().Be("吳美惠");
        }
    }
}