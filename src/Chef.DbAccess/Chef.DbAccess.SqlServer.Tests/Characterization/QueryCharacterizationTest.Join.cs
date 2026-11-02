using System.Linq;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static VerifyMSTest.Verifier;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    public partial class QueryCharacterizationTest
    {
        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_QueryObjet_with_And_Operator()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId && (y.DepId & 1) > 0)
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Departments, (x, y) => x.DepartmentId == y.DepId)
                .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_with_Distinct_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(c => c.Manager, (n, o) => n.ManagerId == o.Id)
                .Where((c, e) => e.Id == 1)
                .Distinct((x, y) => new { x.ManagerId, y.Id, y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_OneToMany_with_Deduplicate_use_QueryObject()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(c => c.Subordinates, (n, o) => n.Id == o.ManagerId)
                .Where((c, e) => c.Id == 1)
                .Select((x, y) => new { x.Id, x.Name, SubordinateId = y.Id, SubordinateName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_Constant_in_On_Condition_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId && (x.DepartmentId == 2 || x.DepartmentId == 3))
                .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_InnerJoin_Two_Tables_use_Property_in_On_Condition_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            var depId = 2;
            var dep = new { Id = 3 };

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId && (x.DepartmentId == depId || x.DepartmentId == dep.Id))
                .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_OrderBy_with_InnerJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .OrderBy((x, y) => y.DepId)
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_use_And_with_InnerJoin_Two_Tables()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .Where((x, y) => new[] { 1, 2 }.Contains(x.Id))
                .And((x, y) => y.DepId == 3)
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_Nested_InnerJoin_Three_Tables()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Three_Tables_OneToMany()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Four_Tables_OneToMany()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_OneToMany()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Six_Tables_OneToMany()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Six_Tables_OneToMany_and_Null_OneToOne()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Seven_Tables_OneToMany()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Subordinates, (x, y) => x.Id == y.ManagerId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Two_Tables_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .Where((x, y) => new[] { 1, 4 }.Contains(x.Id))
                .Select((x, y) => new { x.Id, y.DepId, x.Name, DepartmentName = y.Name })
                .QueryAsync();

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(v => v.Department, (v, w) => v.DepartmentId == w.DepId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_and_Or_Condition_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(v => v.Department, (v, w) => v.DepartmentId == w.DepId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }

        [TestMethod]
        public async Task Test_QueryAsync_with_LeftJoin_Five_Tables_and_OrderByDescending_use_QueryObjet()
        {
            var memberDataAccess = new CapturingDataAccess<User>();

            await memberDataAccess.LeftJoin(v => v.Department, (v, w) => v.DepartmentId == w.DepId)
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

            await Verify(memberDataAccess.CapturedCommands);
        }
    }
}
