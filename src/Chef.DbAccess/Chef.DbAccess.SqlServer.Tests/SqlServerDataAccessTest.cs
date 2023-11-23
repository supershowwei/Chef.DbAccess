using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Chef.DbAccess.Fluent;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests
{
    [TestClass]
    public partial class SqlServerDataAccessTest
    {
        private static readonly IDataAccessFactory DataAccessFactory = SqlServerDataAccessFactory.Instance;

        internal Club Club => new Club { Id = 25 };

        [TestInitialize]
        public void Startup()
        {
            SqlServerDataAccessFactory.Instance.AddConnectionString("Advertisement", @"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Advertisement;Integrated Security=True");
            SqlServerDataAccessFactory.Instance.AddConnectionString("Advertisement2", @"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Advertisement;Integrated Security=True");
            SqlServerDataAccessFactory.Instance.AddConnectionString("Advertisement3", @"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Advertisement;Integrated Security=True");
            SqlServerDataAccessFactory.Instance.AddConnectionString("WantGooConnection", @"Data Source=a.b.c.d;User ID=abc;Password=cba;Initial Catalog=AAA;Max Pool Size=50000");
            SqlServerDataAccessFactory.Instance.AddConnectionString("MallConnection", @"Data Source=d.c.b.a;User ID=abc;Password=cba;Initial Catalog=BBB;Max Pool Size=50000");
        }

        [TestMethod]
        public async Task Test_CountAsync()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var clubCount = await clubDataAccess.Where(x => x.Intro == "陳").CountAsync();

            clubCount.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_CountAsync_with_Join_Two_Tables()
        {
            var userDataAccess = DataAccessFactory.Create<User>();

            var totalUserCount = await userDataAccess.CountAsync(null);
            var userCount = await userDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId).CountAsync();

            totalUserCount.Should().Be(4);
            userCount.Should().Be(3);
        }

        [TestMethod]
        public async Task Test_ExistsAsync()
        {
            var clubDataAccess = DataAccessFactory.Create<Club>();

            var isExists = await clubDataAccess.Where(x => x.Id > 0).ExistsAsync();

            isExists.Should().BeTrue();

            isExists = await clubDataAccess.Where(x => x.Id < 0).ExistsAsync();

            isExists.Should().BeFalse();
        }

        [TestMethod]
        public async Task Test_TransactionScope_Query_and_Update()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.InsertAsync(() => new Club { Id = clubId, Name = "TestClub" });

            Club club;
            using (var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

                club.Name += "989";

                await clubDataAccess.Where(x => x.Id == clubId).Set(() => new Club { Name = club.Name }).UpdateAsync();

                tx.Complete();
            }

            club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            club.Name.Should().Be("TestClub989");
        }

        [TestMethod]
        public async Task Test_TransactionScope_Multiple_Query_and_Update()
        {
            var clubId = new Random(Guid.NewGuid().GetHashCode()).Next(100, 1000);

            var clubDataAccess = DataAccessFactory.Create<Club>();

            await clubDataAccess.InsertAsync(() => new Club { Id = clubId, Name = "TestClub" });

            Club club;
            using (var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

                club.Name += "979";

                await clubDataAccess.Where(x => x.Id == default(int))
                    .Set(() => new Club { Name = default(string) })
                    .UpdateAsync(new List<Club> { new Club { Id = clubId, Name = club.Name } });

                tx.Complete();
            }

            club = await clubDataAccess.Where(x => x.Id == clubId).Select(x => new { x.Id, x.Name }).QueryOneAsync();

            await clubDataAccess.DeleteAsync(x => x.Id == clubId);

            club.Name.Should().Be("TestClub979");
        }
    }

    [ConnectionString(@"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Club;Integrated Security=True")]
    internal class Club
    {
        [Column("ClubID")]
        [Required]
        public int Id { get; set; }

        [StringLength(50)]
        [Required]
        public string Name { get; set; }

        [Required]
        public bool IsActive { get; set; }

        public string Intro { get; set; }

        public DateTime? RunningTime { get; set; }

        public Club Self { get; set; }

        [NotMapped]
        public string IgnoreColumn { get; set; }
    }

    [ConnectionString(@"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Club;Integrated Security=True")]
    internal class IdentityTable
    {
        [Column("SeqNo")]
        public long Id { get; set; }

        [Required]
        public string Name { get; set; }
    }

    [ConnectionString("Advertisement")]
    [ConnectionString("Advertisement2")]
    internal class AdvertisementSetting
    {
        public string Type { get; set; }

        public Guid Id { get; set; }

        public string Image { get; set; }

        public string Link { get; set; }

        public int Weight { get; set; }

        public string AdCode { get; set; }

        public int? OwnerId { get; set; }

        public User Owner { get; set; }
    }

    [Table("Member")]
    internal class AnotherMember
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }

        public string Phone { get; set; }

        public string Address { get; set; }
    }

    [ConnectionString(@"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Member;Integrated Security=True")]
    [Table("Member")]
    internal class User
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }

        public string Phone { get; set; }

        public string Address { get; set; }

        public int SubordinateCount { get; set; }

        public int MaxSubordinateId { get; set; }

        public int SubordinateId { get; set; }

        public string NonExistentProp { get; set; }

        public User Subordinate { get; set; }

        public List<User> Subordinates { get; set; }

        public int DepartmentId { get; set; }

        public Department Department { get; set; }

        public List<Department> Departments { get; set; }

        public int ManagerId { get; set; }

        public User Manager { get; set; }

        public User Self { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            return this.Equals((User)obj);
        }

        public override int GetHashCode()
        {
            if (this.Id == default) return base.GetHashCode();
            
            return this.Id;
        }

        protected bool Equals(User other)
        {
            if (this.Id == default) return ReferenceEquals(this, other);

            return this.Id == other.Id;
        }
    }

    [ConnectionString(@"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Member;Integrated Security=True")]
    [Table("Member")]
    internal class UserStatistics
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string DepartmentName { get; set; }

        public int SubordinateCount { get; set; }

        public int MaxSubordinateId { get; set; }

        public int MinSubordinateId { get; set; }

        public int SumSubordinateAge { get; set; }

        public int TotalCount { get; set; }

        public decimal AvgSubordinateAge { get; set; }

        public int DepartmentId { get; set; }

        public Department Department { get; set; }

        public int ManagerId { get; set; }

        public User Manager { get; set; }

        public User Subordinate { get; set; }
    }

    [ConnectionString(@"Data Source=(LocalDb)\MSSQLLocalDB;Initial Catalog=Member;Integrated Security=True")]
    internal class Department
    {
        [Column("Id")]
        public int DepId { get; set; }

        public string Name { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            return this.Equals((Department)obj);
        }

        public override int GetHashCode()
        {
            if (this.DepId == default) return base.GetHashCode();

            return this.DepId;
        }

        protected bool Equals(Department other)
        {
            if (this.DepId == default) return ReferenceEquals(this, other);

            return this.DepId == other.DepId;
        }
    }

    [ConnectionString("Advertisement3")]
    [Table("AdvertisementSetting")]
    internal class DerivedAdvertisementSetting : AdvertisementSetting
    {
    }

    [ConnectionString("WantGooConnection")]
    internal class Pass
    {
        public long SeqNo { get; set; }

        public string Name { get; set; }

        public int MemberNo { get; set; }

        public DateTime ValidFrom { get; set; }

        public DateTime GoodThru { get; set; }

        public bool IsSubscribed { get; set; }

        public int ProductId { get; set; }

        public int OrderInventoryId { get; set; }

        public DateTime? ApplyForUnsubscription { get; set; }

        public string Note { get; set; }

        public DateTime? UnsubscribedTime { get; set; }

        public Product Product { get; set; }
    }

    [ConnectionString("MallConnection")]
    internal class Product
    {
        [Column("ProductId")]
        public int Id { get; set; }

        public string Name { get; set; }
    }
}
