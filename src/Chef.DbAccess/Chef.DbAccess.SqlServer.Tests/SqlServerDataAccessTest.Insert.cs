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
        public async Task Test_InsertAsync_with_Nonxistence()
        {
            var clubId = 29;

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.InsertAsync(new Club { Id = clubId, Name = "TestClub" }, x => x.Id == clubId);

            result.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_QueryObject_with_Nonxistence()
        {
            var clubId = 29;

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.Set(() => new Club { Id = clubId, Name = "TestClub" }).InsertAsync(x => x.Id == clubId);

            result.Should().Be(0);
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

            identityItem.Id.Should().BeGreaterThan(0);
            identityItem.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_QueryObject_and_DeleteAsync_with_Nonxistence()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.Set(() => new IdentityTable { Name = "Johnny" })
                                   .InsertAsync(x => new { x.Id, x.Name }, x => x.Name == "Johnny");

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            identityItem.Id.Should().BeGreaterThan(0);
            identityItem.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.InsertAsync(new Club { Id = clubId, Name = "TestClub" });

            var club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub");

            club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            club.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Output_and_DeleteAsync()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.InsertAsync(
                                   new IdentityTable { Name = "Johnny" },
                                   x => new { x.Id, x.Name });

            identityItem.Id.Should().BeGreaterThan(0);
            identityItem.Name.Should().Be("Johnny");

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);
        }

        [TestMethod]
        public async Task Test_InsertAsync_use_Setter_with_Nonxistence()
        {
            var clubId = 29;

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.InsertAsync(() => new Club { Id = clubId, Name = "TestClub" }, x => x.Id == clubId);

            result.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Setter()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.InsertAsync(() => new Club { Id = clubId, Name = "TestClub" });

            var club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub");

            club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            club.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Setter_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.InsertAsync(
                                   () => new IdentityTable { Name = "Johnny" },
                                   x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);

            identityItem.Id.Should().BeGreaterThan(0);
            identityItem.Name.Should().Be("Johnny");
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

            identityItem.Id.Should().BeGreaterThan(0);
            identityItem.Name.Should().Be("Johnny");
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Dynamic_Setter()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Set(x => x.Id, clubId).Set(x => x.Name, "TestClub").InsertAsync();

            var club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub");

            club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            club.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_Dynamic_Setter_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItem = await identityTableDataAccess.Set(x => x.Name, "Johnny").InsertAsync(x => new { x.Id, x.Name });

            identityItem.Id.Should().BeGreaterThan(0);
            identityItem.Name.Should().Be("Johnny");

            await identityTableDataAccess.DeleteAsync(x => x.Id == identityItem.Id);
        }

        [TestMethod]
        public void Test_InsertAsync_use_Setter_Has_NotMapped_Column_will_Throw_ArgumentException()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            clubDataAccess
                .Invoking(
                    async dataAccess =>
                        await dataAccess.InsertAsync(() => new Club { Id = clubId, Name = "TestClub", IgnoreColumn = "testabc" }))
                .Should()
                .Throw<ArgumentException>()
                .WithMessage("Member can not applied [NotMapped].");
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_use_QueryObject()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Set(() => new Club { Id = clubId, Name = "TestClub" }).InsertAsync();

            var club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            await clubDataAccess.Where(x => x.Id == clubId).DeleteAsync();

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub");

            club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            club.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.InsertAsync(
                new List<Club>
                {
                    new Club { Id = clubIds[1], Name = "TestClub999", IsActive = true }, new Club { Id = clubIds[0], Name = "TestClub998" }
                });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name, x.IsActive })
                            .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(clubIds[0]);
            clubs[0].IsActive.Should().BeFalse();
            clubs[1].IsActive.Should().BeTrue();
            clubs[1].Name.Should().Be("TestClub999");

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id)).Select(x => new { x.Id, x.Name }).QueryAsync();

            clubs.Should().BeEmpty();
        }

        [TestMethod]
        public async Task Test_InsertAsync_Multiply_with_Nonxistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.InsertAsync(
                             new List<Club>
                             {
                                 new Club { Id = clubIds[1], Name = "TestClub999", IsActive = true },
                                 new Club { Id = clubIds[0], Name = "TestClub998" }
                             },
                             x => x.Id == default);

            result.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_InsertAsync_Multiply_use_QueryObject_with_Nonxistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.Set(() => new Club { Id = default, Name = default, IsActive = default })
                             .InsertAsync(
                                 new List<Club>
                                 {
                                     new Club { Id = clubIds[1], Name = "TestClub999", IsActive = true },
                                     new Club { Id = clubIds[0], Name = "TestClub998" }
                                 },
                                 x => x.Id == default);

            result.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_and_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.InsertAsync(
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
        }

        [TestMethod]
        public async Task Test_InsertAsync_Multiply_use_Setter_with_Nonxistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.InsertAsync(
                             () => new Club { Id = default(int), Name = default(string) },
                             new List<Club>
                             {
                                 new Club { Id = clubIds[1], Name = "TestClub999" }, new Club { Id = clubIds[0], Name = "TestClub998" }
                             },
                             x => x.Id == default);

            result.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_Setter()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.InsertAsync(
                () => new Club { Id = default(int), Name = default(string) },
                new List<Club> { new Club { Id = clubIds[1], Name = "TestClub999" }, new Club { Id = clubIds[0], Name = "TestClub998" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(clubIds[0]);
            clubs[1].Name.Should().Be("TestClub999");

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id)).Select(x => new { x.Id, x.Name }).QueryAsync();

            clubs.Should().BeEmpty();
        }

        [TestMethod]
        public async Task Test_InsertAsync_and_DeleteAsync_Multiply_use_Setter_and_QueryObject()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Set(() => new Club { Id = default(int), Name = default(string) })
                .InsertAsync(new List<Club> { new Club { Id = clubIds[1], Name = "TestClub999" }, new Club { Id = clubIds[0], Name = "TestClub998" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Id.Should().Be(clubIds[0]);
            clubs[1].Name.Should().Be("TestClub999");

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id)).Select(x => new { x.Id, x.Name }).QueryAsync();

            clubs.Should().BeEmpty();
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
        }

        [TestMethod]
        public async Task Test_BulkInsert_with_NonExistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.BulkInsertAsync(
                             new List<Club>
                             {
                                 new Club { Id = clubIds[0], Name = "TestClub1", IsActive = false },
                                 new Club { Id = clubIds[1], Name = "TestClub2", IsActive = true }
                             },
                             x => x.Id == default);

            result.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_BulkInsert()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.BulkInsertAsync(
                () => new Club { Id = default(int), Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Output()
        {
            var identityTableDataAccess = DataAccessFactory.Create<IdentityTable>();

            var identityItems = await identityTableDataAccess.BulkInsertAsync(
                                    new List<IdentityTable> { new IdentityTable { Name = "Johnny" }, new IdentityTable { Name = "Amy" } },
                                    x => new { x.Id, x.Name });

            await identityTableDataAccess.DeleteAsync(x => x.Id >= 0);

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
        }

        [TestMethod]
        public void Test_BulkInsert_without_UserDefinedTable_will_Throw_ArgumentException()
        {
            var advertisementSettingDataAccess = DataAccessFactory.Create<AdvertisementSetting>(null, "Advertisement");

            advertisementSettingDataAccess
                .Invoking(
                    async dataAccess => await advertisementSettingDataAccess.BulkInsertAsync(
                                            () => new AdvertisementSetting { Id = default(Guid) },
                                            new List<AdvertisementSetting>()))
                .Should()
                .Throw<ArgumentException>()
                .WithMessage("Must has UserDefinedAttribute.");
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_QueryObject()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Set(() => new Club { Id = default(int), Name = default(string) })
                .BulkInsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_with_NonExistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.BulkInsertAsync(
                             () => new Club { Id = default(int), Name = default(string) },
                             new List<Club>
                             {
                                 new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                             },
                             x => x.Id == default);

            result.Should().Be(0);
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Setter_and_QueryObject_with_NonExistence()
        {
            var clubIds = new[] { 29, 32 };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var result = await clubDataAccess.Set(() => new Club { Id = default(int), Name = default(string) })
                             .BulkInsertAsync(
                                 new List<Club>
                                 {
                                     new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" }
                                 },
                                 x => x.Id == default);

            result.Should().Be(0);
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
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

            identityItems[0].Id.Should().BeGreaterThan(0);
            identityItems[1].Id.Should().BeGreaterThan(0);
            identityItems[0].Name.Should().Be("Johnny");
            identityItems[1].Name.Should().Be("Amy");
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_Dynamic_Setter()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Set(x => x.Id, default(int))
                .Set(x => x.Name, default(string))
                .BulkInsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");
        }

        [TestMethod]
        public async Task Test_BulkInsert_use_QueryObject_with_RequiredColumns()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.BulkInsertAsync(
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1", IsActive = false },
                    new Club { Id = clubIds[1], Name = "TestClub2", IsActive = true }
                });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name, x.IsActive })
                            .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");
            clubs[0].IsActive.Should().BeFalse();
            clubs[1].IsActive.Should().BeTrue();
        }
    }
}