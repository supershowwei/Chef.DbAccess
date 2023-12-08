using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace Chef.DbAccess.SqlServer.Tests
{
    public partial class SqlServerDataAccessTest
    {
        [TestMethod]
        public async Task Test_UpsertAsync()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.UpsertAsync(x => x.Id == clubId, () => new Club { Name = "TestClub" });

            var club = await clubDataAccess.Where(x => x.Id == clubId)
                           .Select(x => new { x.Id, x.Name, x.IsActive })
                           .QueryOneAsync();

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub");
            club.IsActive.Should().BeTrue();

            await clubDataAccess.UpsertAsync(
                x => x.Id == clubId && x.IsActive == true,
                () => new Club { Name = "TestClub997", IsActive = false });

            club = await clubDataAccess.Where(x => x.Id == clubId)
                       .Select(x => new { x.Id, x.Name, x.IsActive })
                       .QueryOneAsync();

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub997");
            club.IsActive.Should().BeFalse();

            club = await clubDataAccess.Where(x => x.Id == clubId)
                       .Select(x => new { x.Id, x.Name, x.IsActive })
                       .QueryOneAsync();

            club.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_Output()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubInserted = await clubDataAccess.UpsertAsync(
                                   x => x.Id == clubId,
                                   () => new Club { Name = "TestClub" },
                                   x => new { x.Id, x.Name, x.IsActive });

            clubInserted.Id.Should().Be(clubId);
            clubInserted.Name.Should().Be("TestClub");
            clubInserted.IsActive.Should().BeTrue();

            var clubUpserted = await clubDataAccess.Where(x => x.Id == clubId && x.IsActive == true)
                                   .Set(x => x.Name, "TestClub997")
                                   .Set(x => x.IsActive, false)
                                   .UpsertAsync(x => new { x.Id, x.Name, x.IsActive });

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            clubUpserted.Id.Should().Be(clubId);
            clubUpserted.Name.Should().Be("TestClub997");
            clubUpserted.IsActive.Should().BeFalse();

            var club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name, x.IsActive }).QueryOneAsync();

            club.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_Dynamic_Setter()
        {
            var clubDataAccess = SqlServerDataAccessFactory.Instance.Create<Club>();

            var club = await clubDataAccess.Where(x => x.Id == 37).Select(x => new { x.Id, x.Name, x.Intro }).QueryOneAsync();

            var originalName = club.Name;
            var originalIntro = club.Intro;

            await clubDataAccess.Where(x => x.Id == 37).Set(x => x.Name, "test37name").Set(x => x.Intro, "test37intro").UpsertAsync();

            club = await clubDataAccess.Where(x => x.Id == 37).Select(x => new { x.Id, x.Name, x.Intro }).QueryOneAsync();

            club.Name.Should().Be("test37name");
            club.Intro.Should().Be("test37intro");

            await clubDataAccess.Set(x => x.Name, originalName)
                .Set(x => x.Intro, originalIntro)
                .Where(x => x.Id == 37)
                .UpdateAsync();
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_Dynamic_Setter_on_Setter_Is_Null()
        {
            var clubDataAccess = SqlServerDataAccessFactory.Instance.Create<Club>();

            var club = await clubDataAccess.Where(x => x.Id == 37).Select(x => new { x.Id, x.Name, x.Intro }).QueryOneAsync();

            var originalName = club.Name;
            var originalIntro = club.Intro;

            var queryObject = clubDataAccess.Where(x => x.Id == 37);

            queryObject = queryObject.Set(x => x.Name, "test37name").Set(x => x.Intro, "test37intro");

            await queryObject.UpdateAsync();

            club = await clubDataAccess.Where(x => x.Id == 37).Select(x => new { x.Id, x.Name, x.Intro }).QueryOneAsync();

            club.Name.Should().Be("test37name");
            club.Intro.Should().Be("test37intro");

            await clubDataAccess.Set(x => x.Name, originalName)
                .Set(x => x.Intro, originalIntro)
                .Where(x => x.Id == 37)
                .UpdateAsync();
        }

        [TestMethod]
        public async Task Test_UpsertAsync_use_QueryObject()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Where(x => x.Id == clubId).Set(() => new Club { Name = "TestClub" }).UpsertAsync();

            var club = await clubDataAccess.Where(x => x.Id == clubId)
                           .Select(x => new { x.Id, x.Name, x.IsActive })
                           .QueryOneAsync();

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub");
            club.IsActive.Should().BeTrue();

            await clubDataAccess.Where(x => x.Id == clubId && x.IsActive == true)
                .Set(() => new Club { Name = "TestClub997", IsActive = false })
                .UpsertAsync();

            club = await clubDataAccess.Where(x => x.Id == clubId)
                       .Select(x => new { x.Id, x.Name, x.IsActive })
                       .QueryOneAsync();

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            club.Id.Should().Be(clubId);
            club.Name.Should().Be("TestClub997");
            club.IsActive.Should().BeFalse();

            club = await clubDataAccess.Where(x => x.Id == clubId)
                       .Select(x => new { x.Id, x.Name, x.IsActive })
                       .QueryOneAsync();

            club.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_UpsertAsync_Multiply()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.UpsertAsync(
                x => x.Id == default(int),
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");

            await clubDataAccess.UpsertAsync(
                x => x.Id == default(int),
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                        .OrderBy(x => x.Id)
                        .Select(x => new { x.Id, x.Name })
                        .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub3");
            clubs[1].Name.Should().Be("TestClub4");
        }

        [TestMethod]
        public async Task Test_UpsertAsync_Multiply_use_Output()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
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

            clubsInserterd.Count.Should().Be(2);
            clubsInserterd[0].Name.Should().Be("TestClub1");
            clubsInserterd[1].Name.Should().Be("TestClub2");

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

            clubsUpserted.Count.Should().Be(2);
            clubsUpserted[0].Name.Should().Be("TestClub3");
            clubsUpserted[1].Name.Should().Be("TestClub4");
        }

        [TestMethod]
        public async Task Test_UpsertAsync_Multiply_use_QueryObject()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .UpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .UpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                        .OrderBy(x => x.Id)
                        .Select(x => new { x.Id, x.Name })
                        .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub3");
            clubs[1].Name.Should().Be("TestClub4");
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id >= 0 && x.Id <= 0,
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id == default(int),
                () => new Club { Name = default(string) },
                new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                        .OrderBy(x => x.Id)
                        .Select(x => new { x.Id, x.Name })
                        .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub3");
            clubs[1].Name.Should().Be("TestClub4");
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync_with_Properties_has_Same_Type()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id >= 0 && x.Id <= 0,
                () => new Club { Name = default, Intro = default },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub1", Intro = "TestClub1_Intro" },
                    new Club { Id = clubIds[1], Name = "TestClub2", Intro = "TestClub2_Intro" }
                });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name, x.Intro })
                            .QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[0].Intro.Should().Be("TestClub1_Intro");
            clubs[1].Name.Should().Be("TestClub2");
            clubs[1].Intro.Should().Be("TestClub2_Intro");

            await clubDataAccess.BulkUpsertAsync(
                x => x.Id == default,
                () => new Club { Name = default, Intro = default },
                new List<Club>
                {
                    new Club { Id = clubIds[0], Name = "TestClub3", Intro = "TestClub3_Intro" },
                    new Club { Id = clubIds[1], Name = "TestClub4", Intro = "TestClub4_Intro" }
                });

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                        .OrderBy(x => x.Id)
                        .Select(x => new { x.Id, x.Name, x.Intro })
                        .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub3");
            clubs[0].Intro.Should().Be("TestClub3_Intro");
            clubs[1].Name.Should().Be("TestClub4");
            clubs[1].Intro.Should().Be("TestClub4_Intro");
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync_use_Output()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
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

            clubsInserted.Count.Should().Be(2);
            clubsInserted[0].Name.Should().Be("TestClub1");
            clubsInserted[1].Name.Should().Be("TestClub2");

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

            clubsUpserted.Count.Should().Be(2);
            clubsUpserted[0].Name.Should().Be("TestClub3");
            clubsUpserted[1].Name.Should().Be("TestClub4");
        }

        [TestMethod]
        public async Task Test_BulkUpsertAsync_use_QueryObject()
        {
            var clubIds = new[]
                          {
                              new Random(Guid.NewGuid().GetHashCode()).Next(100, 500),
                              new Random(Guid.NewGuid().GetHashCode()).Next(500, 1000)
                          };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Where(x => x.Id >= default(int) && x.Id <= default(int))
                .Set(() => new Club { Name = default(string) })
                .BulkUpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub1" }, new Club { Id = clubIds[1], Name = "TestClub2" } });

            var clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                            .OrderBy(x => x.Id)
                            .Select(x => new { x.Id, x.Name })
                            .QueryAsync();

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub1");
            clubs[1].Name.Should().Be("TestClub2");

            await clubDataAccess.Where(x => x.Id == default(int))
                .Set(() => new Club { Name = default(string) })
                .BulkUpsertAsync(
                    new List<Club> { new Club { Id = clubIds[0], Name = "TestClub3" }, new Club { Id = clubIds[1], Name = "TestClub4" } });

            clubs = await clubDataAccess.Where(x => clubIds.Contains(x.Id))
                        .OrderBy(x => x.Id)
                        .Select(x => new { x.Id, x.Name })
                        .QueryAsync();

            await clubDataAccess.DeleteAsync(x => clubIds.Contains(x.Id));

            clubs.Count.Should().Be(2);
            clubs[0].Name.Should().Be("TestClub3");
            clubs[1].Name.Should().Be("TestClub4");
        }
    }
}