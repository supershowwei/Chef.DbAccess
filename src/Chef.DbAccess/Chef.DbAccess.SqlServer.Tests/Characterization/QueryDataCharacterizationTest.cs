using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VerifyMSTest;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    /// <summary>
    /// 3.x QueryAsync 的資料快照：連 LocalDB 執行與 SqlServerDataAccessTest.Query.cs 相同的情境，
    /// 記錄查詢結果，給 5.0 改寫後比對資料是否一致。測試名稱與整合測試一一對應。
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [UsesVerify]
    public partial class QueryDataCharacterizationTest
    {
        private static readonly IDataAccessFactory DataAccessFactory = SqlServerDataAccessFactory.Instance;

        [TestInitialize]
        public Task Startup()
        {
            return SqlServerDataAccessTest.RestoreSeedDataAsync();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_GroupBy_and_OrderBy()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.GroupBy(x => new { x.Id }, g => new Club { Id = g.Select(x => x.Id) })
                            .OrderBy(x => x.Id)
                            .QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            await Verify(result).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_QueryObjet_with_And_Operator()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId && (y.DepId & 1) > 0)
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            await Verify(result).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Departments, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            await Verify(result).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_with_Distinct_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(c => c.Manager, (n, o) => n.ManagerId == o.Id)
                             .Where((c, e) => e.Id == 1)
                             .Distinct((x, y) => new { x.ManagerId, y.Id, y.Name })
                             .QueryAsync();

            await Verify(result).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_with_Deduplicate_use_QueryObject()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(c => c.Subordinates, (n, o) => n.Id == o.ManagerId)
                             .Where((c, e) => c.Id == 1)
                             .Select((x, y) => new { x.Id, x.Name, SubordinateId = y.Id, SubordinateName = y.Name })
                             .QueryAsync();

            await Verify(result).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_Constant_in_On_Condition_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId && (x.DepartmentId == 2 || x.DepartmentId == 3))
                             .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = DataAccessFactory.Create<User>();

            var result = await memberDataAccess.LeftJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                             .Where((x, y) => new[] { 1, 4 }.Contains(x.Id))
                             .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                             .QueryAsync();

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
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

            await Verify(result).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Null()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.Intro == null).Select(x => new { x.Id }).QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.QueryAsync(x => new[] { 17, 25 }.Contains(x.Id), selector: x => new { x.Name });

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_And_Operator()
        {
            var userDataAccess = DataAccessFactory.Create<User>();

            var users = await userDataAccess.Where(x => (x.DepartmentId & x.ManagerId) == 0).Select(x => new { x.Id, x.Name }).QueryAsync();

            var otherUsers = await userDataAccess.Where(x => (x.DepartmentId & x.ManagerId) > 0).Select(x => new { x.Id, x.Name }).QueryAsync();

            await Verify(new { users, otherUsers }).AsDataSnapshot();
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

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_Empty_List_Contains()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var ids = new List<int>();

            var clubs = await clubDataAccess.Where(x => ids.Contains(x.Id)).Select(x => new { x.Name }).QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_use_Pagination_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(3).Take(2).QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id)).Select(x => new { x.Name }).QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_use_Pagination_only_Skip_QueryObject()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Select(x => new { x.Id, x.Name }).OrderBy(x => x.Id).Skip(26).QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_has_DateTime_Now()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.RunningTime < DateTime.Now).Select(x => new { x.Id, x.Name }).QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_And()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.RunningTime < DateTime.Now)
                            .And(y => y.IsActive == false)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_Or()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => x.RunningTime > DateTime.Now)
                            .Or(y => y.Name == "王真希")
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_OrderByDescending()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                            .OrderByDescending(x => x.Id)
                            .Select(x => new { x.Name })
                            .QueryAsync();

            await Verify(clubs).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Selector_use_QueryObject_and_Take_without_Ordering()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubs = await clubDataAccess.Where(x => new[] { 17, 25 }.Contains(x.Id))
                            .Select(x => new { x.Name })
                            .Take(1)
                            .QueryAsync();

            await Verify(clubs).AsDataSnapshot();
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

            await Verify(clubs).AsDataSnapshot();
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

            await Verify(clubs).AsDataSnapshot();
        }
    }
}
