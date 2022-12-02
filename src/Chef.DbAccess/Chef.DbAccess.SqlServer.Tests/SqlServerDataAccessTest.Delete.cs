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
    public partial class SqlServerDataAccessTest
    {
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

            var deletedUser = deleted.First();

            deletedUser.Id.Should().Be(user.Id);
            deletedUser.Name.Should().Be(user.Name);
            deletedUser.Age.Should().Be(user.Age);
            deletedUser.Phone.Should().Be(user.Phone);
            deletedUser.Address.Should().Be(user.Address);
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Two_Tables()
        {
            var userDataAccess = DataAccessFactory.Create<User>();

            await userDataAccess.Where(x => new[] { 66, 77 }.Contains(x.Id)).DeleteAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 66,
                        Name = "666",
                        Age = 66,
                        Phone = "0966666666",
                        Address = "六六六六六六",
                        DepartmentId = -1,
                        ManagerId = 3
                    })
                .InsertAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 77,
                        Name = "777",
                        Age = 77,
                        Phone = "0977777777",
                        Address = "七七七七七七七",
                        DepartmentId = -1,
                        ManagerId = 3
                    })
                .InsertAsync();

            var users = await userDataAccess.Where(x => new[] { 66, 77 }.Contains(x.Id)).Select(x => new { x.Id }).QueryAsync();

            users.Count.Should().Be(2);

            await userDataAccess.InnerJoin(x => x.Manager, (x, y) => x.ManagerId == y.Id).Where((x, y) => y.Id == 3).DeleteAsync();

            users = await userDataAccess.Where(x => new[] { 66, 77 }.Contains(x.Id)).Select(x => new { x.Id }).QueryAsync();

            users.Count.Should().Be(0);
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

            foreach (var deletedUser in deleted)
            {
                var user = users.Single(x => x.Id == deletedUser.Id);

                deletedUser.Id.Should().Be(user.Id);
                deletedUser.Name.Should().Be(user.Name);
                deletedUser.Age.Should().Be(user.Age);
                deletedUser.Phone.Should().Be(user.Phone);
                deletedUser.Address.Should().Be(user.Address);
            }
        }

        [TestMethod]
        public async Task Test_DeleteAsync_with_Join_Three_Tables()
        {
            var userDataAccess = DataAccessFactory.Create<User>();

            await userDataAccess.Where(x => new[] { 555, 666 }.Contains(x.Id)).DeleteAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 555,
                        Name = "5555",
                        Age = 555,
                        Phone = "0955555555",
                        Address = "五五五五五",
                        DepartmentId = 2,
                        ManagerId = 3
                    })
                .InsertAsync();

            await userDataAccess.Set(
                    () => new User
                    {
                        Id = 666,
                        Name = "6666",
                        Age = 666,
                        Phone = "0966666666",
                        Address = "六六六六六六",
                        DepartmentId = 2,
                        ManagerId = 4
                    })
                .InsertAsync();

            var users = await userDataAccess.Where(x => new[] { 555, 666 }.Contains(x.Id)).Select(x => new { x.Id }).QueryAsync();

            users.Count.Should().Be(2);

            await userDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.DepId)
                .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                .Where((x, y, z) => y.DepId == 2 && z.Id == 3)
                .DeleteAsync();

            users = await userDataAccess.Where(x => new[] { 555, 666 }.Contains(x.Id)).Select(x => new { x.Id }).QueryAsync();

            users.Count.Should().Be(1);

            await userDataAccess.Where(x => new[] { 555, 666 }.Contains(x.Id)).DeleteAsync();
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

            foreach (var deletedUser in deleted)
            {
                var user = users.Single(x => x.Id == deletedUser.Id);

                deletedUser.Id.Should().Be(user.Id);
                deletedUser.Name.Should().Be(user.Name);
                deletedUser.Age.Should().Be(user.Age);
                deletedUser.Phone.Should().Be(user.Phone);
                deletedUser.Address.Should().Be(user.Address);
            }

            await userDataAccess.Where(x => new[] { 5555, 6666 }.Contains(x.Id)).DeleteAsync();
        }
    }
}
