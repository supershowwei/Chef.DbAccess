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
    /// 3.x 寫入方法帶 Output 時回傳結果的資料快照：連 LocalDB 執行與 SqlServerDataAccessTest 的
    /// Insert.cs、Delete.cs、Upsert.cs 相同的情境（含事前準備與事後清除），記錄 Output 的結果。
    /// 測試名稱與整合測試一一對應；亂數 Id 改成固定值，IdentityTable 的識別值每次不同，予以遮蔽。
    /// </summary>
    [TestClass]
    [TestCategory("Integration")]
    [UsesVerify]
    public partial class OutputDataCharacterizationTest
    {
        private static readonly IDataAccessFactory DataAccessFactory = SqlServerDataAccessFactory.Instance;

        [TestInitialize]
        public async Task Startup()
        {
            // 與 SqlServerDataAccessTest.Startup 相同：清除上次留下的暫存 Club，並還原種子資料
            await DataAccessFactory.Create<Club>().Where(x => x.Id >= 100).DeleteAsync();

            await SqlServerDataAccessTest.RestoreSeedDataAsync();
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_DeleteAsync_with_Nonxistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.InsertAsync(
                                   new IdentityTable { Name = "Johnny" },
                                   x => new { x.Id, x.Name },
                                   x => x.Name == "Johnny");

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            await Verify(identityItem).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_QueryObject_and_DeleteAsync_with_Nonxistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.Set(() => new IdentityTable { Name = "Johnny" })
                                   .InsertAsync(x => new { x.Id, x.Name }, x => x.Name == "Johnny");

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            await Verify(identityItem).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_DeleteAsync()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.InsertAsync(
                                   new IdentityTable { Name = "Johnny" },
                                   x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            await Verify(identityItem).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Setter_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.InsertAsync(
                                   () => new IdentityTable { Name = "Johnny" },
                                   x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            await Verify(identityItem).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Setter_and_Output_with_Nonxistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.InsertAsync(
                                   () => new IdentityTable { Name = "Johnny" },
                                   x => new { x.Id, x.Name },
                                   x => x.Name == "Johnny");

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            await Verify(identityItem).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Dynamic_Setter_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.Set(x => x.Name, "Johnny").InsertAsync(x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            await Verify(identityItem).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.InsertAsync(
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_and_Output_with_Nonxistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.InsertAsync(
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name },
                                    x => x.Name == default);

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_QueryObject_and_Output_with_Nonxistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                                    .InsertAsync(
                                        new List<IdentityTable>
                                        {
                                            new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                                        },
                                        x => new { x.Id, x.Name },
                                        x => x.Name == default);

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_QueryObject_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                                    .InsertAsync(
                                        new List<IdentityTable>
                                        {
                                            new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                                        },
                                        x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.BulkInsertAsync(
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Output_with_NonExistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.BulkInsertAsync(
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name },
                                    x => x.Name == default);

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_Output_NonExistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.BulkInsertAsync(
                                    () => new IdentityTable { Name = default },
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name },
                                    x => x.Name == default);

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_QueryObject_and_Output_NonExistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                                    .BulkInsertAsync(
                                        new List<IdentityTable>
                                        {
                                            new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                                        },
                                        x => new { x.Id, x.Name },
                                        x => x.Name == default);

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.BulkInsertAsync(
                                    () => new IdentityTable { Name = default },
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_QueryObject_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.Set(() => new IdentityTable { Name = default })
                                    .BulkInsertAsync(
                                        new List<IdentityTable>
                                        {
                                            new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" }
                                        },
                                        x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            await Verify(identityItems).AsDataSnapshot().ScrubMember<IdentityTable>(x => x.Id);
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Output()
        {
            var userDataAccess = DataAccessFactory.Create<User>();

            await userDataAccess.Where(x => x.Id == 55).DeleteAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 55,
                        Name = "555",
                        Age = 55,
                        Phone = "0955555555",
                        Address = "五五五五五",
                        DepartmentId = -1,
                        ManagerId = 33
                    })
                .InsertAsync();

            var user = await userDataAccess.Where(x => x.Id == 55)
                           .Select(
                               x => new
                               {
                                   x.Id,
                                   x.Name,
                                   x.Age,
                                   x.Phone,
                                   x.Address
                               })
                           .QueryOneAsync();

            var deleted = await userDataAccess.Where(x => x.Id == 55)
                         .DeleteAsync(
                             x => new
                             {
                                 x.Id,
                                 x.Name,
                                 x.Age,
                                 x.Phone,
                                 x.Address
                             });

            await Verify(deleted).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Two_Tables_and_Output()
        {
            var userDataAccess = DataAccessFactory.Create<User>();

            await userDataAccess.Where(x => new[] { 88, 99 }.Contains(x.Id)).DeleteAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 88,
                        Name = "888",
                        Age = 88,
                        Phone = "0988888888",
                        Address = "八八八八八八八八",
                        DepartmentId = -1,
                        ManagerId = 3
                    })
                .InsertAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 99,
                        Name = "999",
                        Age = 99,
                        Phone = "0999999999",
                        Address = "九九九九九九九九九",
                        DepartmentId = -1,
                        ManagerId = 3
                    })
                .InsertAsync();

            var users = await userDataAccess.Where(x => new[] { 88, 99 }.Contains(x.Id))
                            .Select(
                                x => new
                                {
                                    x.Id,
                                    x.Name,
                                    x.Age,
                                    x.Phone,
                                    x.Address
                                })
                            .QueryAsync();

            var deleted = await userDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id)
                             .Where((x, y) => y.Id == 3)
                             .DeleteAsync(
                                 x => new
                                 {
                                     x.Id,
                                     x.Name,
                                     x.Age,
                                     x.Phone,
                                     x.Address
                                 });

            await Verify(deleted).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Three_Tables_and_Output()
        {
            var userDataAccess = DataAccessFactory.Create<User>();

            await userDataAccess.Where(x => new[] { 5555, 6666 }.Contains(x.Id)).DeleteAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 5555,
                        Name = "55555",
                        Age = 5555,
                        Phone = "0955555555",
                        Address = "五五五五五",
                        DepartmentId = 2,
                        ManagerId = 3
                    })
                .InsertAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 6666,
                        Name = "66666",
                        Age = 6666,
                        Phone = "0966666666",
                        Address = "六六六六六六",
                        DepartmentId = 2,
                        ManagerId = 4
                    })
                .InsertAsync();

            var users = await userDataAccess.Where(x => new[] { 5555, 6666 }.Contains(x.Id))
                            .Select(
                                x => new
                                {
                                    x.Id,
                                    x.Name,
                                    x.Age,
                                    x.Phone,
                                    x.Address
                                })
                            .QueryAsync();

            var deleted = await userDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                              .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                              .Where((x, y, z) => y.DepId == 2 && z.Id == 3)
                              .DeleteAsync(
                                  x => new
                                  {
                                      x.Id,
                                      x.Name,
                                      x.Age,
                                      x.Phone,
                                      x.Address
                                  });

            await userDataAccess.Where(x => new[] { 5555, 6666 }.Contains(x.Id)).DeleteAsync();

            await Verify(deleted).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_Output()
        {
            var clubId = 101;

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubInserted = await clubDataAccess.UpsertAsync(
                                   x => x.Id == clubId,
                                   () => new Club { Name = "TestClub" },
                                   x => new { x.Id, x.Name, x.IsActive });

            var clubUpserted = await clubDataAccess.Where(x => x.Id == clubId && x.IsActive == true)
                                   .Set(x => x.Name, "TestClub997")
                                   .Set(x => x.IsActive, false)
                                   .UpsertAsync(x => new { x.Id, x.Name, x.IsActive });

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            var club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name, x.IsActive }).QueryOneAsync();

            await Verify(new { clubInserted, clubUpserted }).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_UpsertAsync_Multiply_use_Output()
        {
            var clubIds = new[]
                          {
                              101,
                              501
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubsInserterd = await clubDataAccess.UpsertAsync(
                                     x => x.Id == default(int),
                                     () => new Club { Name = default(string) },
                                     new List<Club>
                                     {
                                         new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                                     },
                                     x => new { x.Id, x.Name });

            var clubsUpserted = await clubDataAccess.Where(x => x.Id == default(int))
                                    .Set(() => new Club { Name = default(string) })
                                    .UpsertAsync(
                                        new List<Club>
                                        {
                                            new Club { Id = clubIds[0], Name = "TestClub3" },
                                            new Club { Id = clubIds[1], Name = "TestClub4" }
                                        },
                                        x => new { x.Id, x.Name });

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            await Verify(new { clubsInserterd, clubsUpserted }).AsDataSnapshot();
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync_use_Output()
        {
            var clubIds = new[]
                          {
                              101,
                              501
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubsInserted = await clubDataAccess.BulkUpsertAsync(
                                    x => x.Id >= 0 && x.Id <= 0,
                                    () => new Club { Name = default(string) },
                                    new List<Club>
                                    {
                                        new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                                    },
                                    x => new { x.Id, x.Name });

            var clubsUpserted = await clubDataAccess.Where(x => x.Id == default(int))
                                    .Set(() => new Club { Name = default(string) })
                                    .BulkUpsertAsync(
                                        new List<Club>
                                        {
                                            new Club { Id = clubIds[0], Name = "TestClub3" },
                                            new Club { Id = clubIds[1], Name = "TestClub4" }
                                        },
                                        x => new { x.Id, x.Name });

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            await Verify(new { clubsInserted, clubsUpserted }).AsDataSnapshot();
        }
    }
}
