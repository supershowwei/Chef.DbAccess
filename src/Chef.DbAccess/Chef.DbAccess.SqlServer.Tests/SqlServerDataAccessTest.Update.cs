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
        public async Task Test_UpdateAsync()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubName = "歐陽邦瑋" + suffix;

            await clubDataAccess.UpdateAsync(x => x.Id.Equals(15), () => new Club { Name = clubName });

            var club = await clubDataAccess.QueryOneAsync(x => x.Id == 15, null, x => new { x.Id, x.Name });

            club.Id.Should().Be(15);
            club.Name.Should().Be("歐陽邦瑋" + suffix);
        }

        [TestMethod]
        public void Test_UpdateAsync_use_NotMapped_Column_will_Throw_ArgumentException()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubName = "歐陽邦瑋" + suffix;

            clubDataAccess
                .Invoking(
                    async dataAccess => await dataAccess.UpdateAsync(
                                            x => x.Id.Equals(15),
                                            () => new Club { Name = clubName, IgnoreColumn = "testabc" }))
                .Should()
                .Throw<ArgumentException>()
                .WithMessage("Member can not applied [NotMapped].");
        }

        [TestMethod]
        public async Task Test_UpdateAsync_set_Null()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var club = await clubDataAccess.QueryOneAsync(x => x.Id == 36, selector: x => new { x.Id, x.Intro });

            club.Id.Should().Be(36);
            club.Intro.Should().Be("連");

            await clubDataAccess.UpdateAsync(x => x.Id.Equals(36), () => new Club { Intro = null });

            club = await clubDataAccess.QueryOneAsync(x => x.Id == 36, selector: x => new { x.Id, x.Intro });

            await clubDataAccess.UpdateAsync(x => x.Id.Equals(36), () => new Club { Intro = "連" });

            club.Id.Should().Be(36);
            club.Intro.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_UpdateAsync_use_QueryObject()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubName = "歐陽邦瑋" + suffix;

            await clubDataAccess.Where(x => x.Id.Equals(15)).Set(() => new Club { Name = clubName }).UpdateAsync();

            var club = await clubDataAccess.QueryOneAsync(x => x.Id == 15, null, x => new { x.Id, x.Name });

            club.Id.Should().Be(15);
            club.Name.Should().Be("歐陽邦瑋" + suffix);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_use_Dynamic_Setter()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubName = "歐陽邦瑋" + suffix;

            await clubDataAccess.Where(x => x.Id.Equals(15)).Set(x => x.Name, clubName).UpdateAsync();

            var club = await clubDataAccess.QueryOneAsync(x => x.Id == 15, null, x => new { x.Id, x.Name });

            club.Id.Should().Be(15);
            club.Name.Should().Be("歐陽邦瑋" + suffix);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_use_QueryObject_set_Null()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var club = await clubDataAccess.QueryOneAsync(x => x.Id == 36, selector: x => new { x.Id, x.Intro });

            club.Id.Should().Be(36);
            club.Intro.Should().Be("連");

            await clubDataAccess.Where(x => x.Id == 36).Set(() => new Club { Intro = null }).UpdateAsync();

            club = await clubDataAccess.Where(x => x.Id == 36).Select(x => new { x.Id, x.Intro }).QueryOneAsync();

            await clubDataAccess.Where(x => x.Id == 36).Set(() => new Club { Intro = "連" }).UpdateAsync();

            club.Id.Should().Be(36);
            club.Intro.Should().BeNull();
        }

        [TestMethod]
        public async Task Test_UpdateAsync_Multiply()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + suffix },
                            new Club { Id = 16, Name = "羅怡君" + suffix },
                            new Club { Id = 19, Name = "楊翊貴" + suffix }
                        };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.UpdateAsync(x => x.Id == default(int), () => new Club { Name = default(string) }, clubs);

            var actual = await clubDataAccess.QueryAsync(x => new[] { 15, 16, 19 }.Contains(x.Id), selector: x => new { x.Id, x.Name });

            actual.Single(x => x.Id.Equals(15)).Name.Should().Be("歐陽邦瑋" + suffix);
            actual.Single(x => x.Id.Equals(16)).Name.Should().Be("羅怡君" + suffix);
            actual.Single(x => x.Id.Equals(19)).Name.Should().Be("楊翊貴" + suffix);
        }

        [TestMethod]
        public async Task Test_UpdateAsync_Multiply_use_QueryObject()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + suffix },
                            new Club { Id = 16, Name = "羅怡君" + suffix },
                            new Club { Id = 19, Name = "楊翊貴" + suffix }
                        };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Where(x => x.Id == default(int)).Set(() => new Club { Name = default(string) }).UpdateAsync(clubs);

            var actual = await clubDataAccess.QueryAsync(x => new[] { 15, 16, 19 }.Contains(x.Id), selector: x => new { x.Id, x.Name });

            actual.Single(x => x.Id.Equals(15)).Name.Should().Be("歐陽邦瑋" + suffix);
            actual.Single(x => x.Id.Equals(16)).Name.Should().Be("羅怡君" + suffix);
            actual.Single(x => x.Id.Equals(19)).Name.Should().Be("楊翊貴" + suffix);
        }

        [TestMethod]
        public async Task Test_BulkUpdate()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + suffix },
                            new Club { Id = 16, Name = "羅怡君" + suffix },
                            new Club { Id = 19, Name = "楊翊貴" + suffix }
                        };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.BulkUpdateAsync(x => x.Id == default(int), () => new Club { Name = default(string) }, clubs);

            var actual = await clubDataAccess.QueryAsync(x => new[] { 15, 16, 19 }.Contains(x.Id), selector: x => new { x.Id, x.Name });

            actual.Single(x => x.Id.Equals(15)).Name.Should().Be("歐陽邦瑋" + suffix);
            actual.Single(x => x.Id.Equals(16)).Name.Should().Be("羅怡君" + suffix);
            actual.Single(x => x.Id.Equals(19)).Name.Should().Be("楊翊貴" + suffix);
        }

        [TestMethod]
        public async Task Test_BulkUpdate_use_QueryObject()
        {
            var suffix = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000).ToString();

            var clubs = new List<Club>
                        {
                            new Club { Id = 15, Name = "歐陽邦瑋" + suffix },
                            new Club { Id = 16, Name = "羅怡君" + suffix },
                            new Club { Id = 19, Name = "楊翊貴" + suffix }
                        };

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.Where(x => x.Id == default(int)).Set(() => new Club { Name = default(string) }).BulkUpdateAsync(clubs);

            var actual = await clubDataAccess.QueryAsync(x => new[] { 15, 16, 19 }.Contains(x.Id), selector: x => new { x.Id, x.Name });

            actual.Single(x => x.Id.Equals(15)).Name.Should().Be("歐陽邦瑋" + suffix);
            actual.Single(x => x.Id.Equals(16)).Name.Should().Be("羅怡君" + suffix);
            actual.Single(x => x.Id.Equals(19)).Name.Should().Be("楊翊貴" + suffix);
        }
    }
}