using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Simple.Data.Firebird.Test
{
    public class FindTest : IClassFixture<DbHelper>
    {
        private dynamic _db;

        public FindTest(DbHelper helper)
        {
            _db = helper.OpenDefault();
        }

        [Fact]
        public void TestFindByName()
        {

            var user = _db.Public.Users.FindByName("Bob");
            Assert.NotNull(user);
            Assert.Equal("Bob", user.Name);
        }

        [Fact]
        public void TestFindByNameWithCast()
        {

            var user = (User)_db.Public.Users.FindByName("Bob");
            Assert.NotNull(user);
            Assert.Equal("Bob", user.Name);
        }

        [Fact]
        public void TestFindAllByName()
        {

            IEnumerable<User> users = _db.Users.FindAllByName("Bob").Cast<User>();
            Assert.Equal(1, users.Count());
        }

        [Fact]
        public void TestFindAllByNameAsIEnumerableOfDynamic()
        {

            IEnumerable<dynamic> users = _db.Users.FindAllByName("Bob");
            Assert.Equal(1, users.Count());
        }

        [Fact]
        public void TestFindAllByPartialName()
        {
            IEnumerable<User> users = _db.Users.FindAll(_db.Users.Name.Like("Bob")).ToList<User>();
            Assert.Equal(1, users.Count());
        }

        [Fact]
        public void TestAllCount()
        {

            var count = _db.Users.All().ToList().Count;
            Assert.Equal(3, count);
        }

        [Fact]
        public void TestAllWithSkipCount()
        {

            var count = _db.Users.All().Skip(1).ToList().Count;
            Assert.Equal(2, count);
        }

        [Fact]
        public void TestAllWithSkipTake()
        {

            List<User> users = _db.Users.All().Skip(1).Take(2).ToList<User>();
            Assert.Equal(2, users.Count);
            Assert.Equal("Charlie", users[0].Name);
            Assert.Equal("Dave", users[1].Name);
        }

        [Fact]
        public void TestAllWithSkipTakeTotalCount()
        {
            Future<int> totalCount;
            var count = _db.Users.All().WithTotalCount(out totalCount).Skip(1).Take(2).ToList().Count;
            Assert.Equal(3, totalCount);
            Assert.Equal(2, count);
        }

        [Fact]
        public void TestAllWhereWithSkipTakeTotalCount()
        {

            Future<int> totalCount;
            List<User> users = _db.Users.All().Where(_db.Users.Name.Like("%e")).WithTotalCount(out totalCount).Skip(1).ToList<User>();
            Assert.Equal(2, totalCount);
            Assert.Equal(1, users.Count);
            Assert.Equal("Dave", users[0].Name);
        }

        [Fact]
        public void TestImplicitCast()
        {

            User user = _db.Users.FindByName("Bob");
            Assert.NotNull(user);
        }

        [Fact]
        public void TestImplicitEnumerableCast()
        {

            foreach (User user in _db.Users.All())
            {
                Assert.NotNull(user);
            }
        }

        [Fact]
        public void TestFindOnAView()
        {
            var customer = _db.ViewCustomers.FindByName("Test");
            Assert.NotNull(customer);
        }

        [Fact]
        public void TestCast()
        {
            var userQuery = _db.Users.All().Cast<User>() as IEnumerable<User>;
            Assert.NotNull(userQuery);
            var users = userQuery.ToList();
            Assert.NotEqual(0, users.Count);
        }
    }
}
