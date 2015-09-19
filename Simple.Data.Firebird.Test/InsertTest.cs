using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace Simple.Data.Firebird.Test
{
    public class InsertTest : IClassFixture<DbHelper>
    {
        private dynamic _db;

        public InsertTest(DbHelper helper)
        {
            _db = helper.OpenDefault();
        }


        #region MA - added support for inserting enums

        internal class UserWithEnum
        {
            public int? Id { get; set; }
            public string Name { get; set; }
            public string Password { get; set; }
            public AgeDescription Age { get; set; }
        }

        public enum AgeDescription
        {
            Young = 1,
            Old = 80
        }

        [Fact]
        public void TestInsertWithStaticTypeObjectWithEnum()
        {
            var user = new UserWithEnum
            {
                Name = "Zaphod",
                Password = "zarquon",
                Age = AgeDescription.Old
            };

            UserWithEnum actual = _db.Users.Insert(user);

            Assert.NotNull(user);
            Assert.NotEqual(0, actual.Id);
            Assert.Equal("Zaphod", actual.Name);
            Assert.Equal("zarquon", actual.Password);
            Assert.Equal(80, (int)actual.Age);
            Assert.Equal(AgeDescription.Old, actual.Age);
        }

        [Fact]
        public void TestInsertManyWithStaticTypeObjectWithEnum()
        {
            var user1 = new UserWithEnum
            {
                Name = "Zaphod",
                Password = "zarquon",
                Age = AgeDescription.Old
            };
            var user2 = new UserWithEnum
            {
                Name = "Zaphod2",
                Password = "zarquon2",
                Age = AgeDescription.Young
            };

            List<UserWithEnum> actual = _db.Users.Insert(new[] { user1, user2 }).ToList<UserWithEnum>();

            Assert.NotNull(actual[0]);
            Assert.NotEqual(0, actual[0].Id);
            Assert.Equal("Zaphod", actual[0].Name);
            Assert.Equal("zarquon", actual[0].Password);
            Assert.Equal(80, (int)actual[0].Age);
            Assert.Equal(AgeDescription.Old, actual[0].Age);

            Assert.NotNull(actual[1]);
            Assert.NotEqual(0, actual[1].Id);
            Assert.Equal("Zaphod2", actual[1].Name);
            Assert.Equal("zarquon2", actual[1].Password);
            Assert.Equal(1, (int)actual[1].Age);
            Assert.Equal(AgeDescription.Young, actual[1].Age);
        }

        [Fact]
        public void TestInsertWithObjectWithNullValue()
        {
            var actual = _db.Customers.Insert(Name: "customername", Address: null);

            Assert.Equal("customername", actual.Name);
            Assert.Null(actual.Address);
        }

        #endregion

        [Fact]
        public void TestInsertWithNamedArguments()
        {
            var user = _db.Public.Users.Insert(Name: "Ford", Password: "hoopy", Age: 29);

            Assert.NotNull(user);
            Assert.Equal("Ford", user.Name);
            Assert.Equal("hoopy", user.Password);
            Assert.Equal(29, user.Age);
        }

        [Fact]
        public void TestInsertWithStaticTypeObject()
        {
            var user = new User { Name = "Zaphod", Password = "zarquon", Age = 42 };

            var actual = _db.Users.Insert(user);

            Assert.NotNull(user);
            Assert.NotEqual(0, actual.Id);
            Assert.Equal("Zaphod", actual.Name);
            Assert.Equal("zarquon", actual.Password);
            Assert.Equal(42, actual.Age);
        }

        [Fact]
        public void TestMultiInsertWithStaticTypeObjects()
        {
            var users = new[]
                  {
                    new User {Name = "Slartibartfast", Password = "bistromathics", Age = 777},
                    new User {Name = "Wowbagger", Password = "teatime", Age = int.MaxValue}
                  };

            IList<User> actuals = _db.Users.Insert(users).ToList<User>();

            Assert.Equal(2, actuals.Count);
            Assert.NotEqual(0, actuals[0].Id);
            Assert.Equal("Slartibartfast", actuals[0].Name);
            Assert.Equal("bistromathics", actuals[0].Password);
            Assert.Equal(777, actuals[0].Age);

            Assert.NotEqual(0, actuals[1].Id);
            Assert.Equal("Wowbagger", actuals[1].Name);
            Assert.Equal("teatime", actuals[1].Password);
            Assert.Equal(int.MaxValue, actuals[1].Age);
        }

        [Fact]
        public void TestInsertWithDynamicTypeObject()
        {
            dynamic user = new ExpandoObject();
            user.Name = "Marvin";
            user.Password = "diodes";
            user.Age = 42000000;

            var actual = _db.Users.Insert(user);

            Assert.NotNull(user);
            Assert.Equal("Marvin", actual.Name);
            Assert.Equal("diodes", actual.Password);
            Assert.Equal(42000000, actual.Age);
        }

        [Fact]
        public void TestMultiInsertWithDynamicTypeObjects()
        {
            dynamic user1 = new ExpandoObject();
            user1.Name = "Slartibartfast";
            user1.Password = "bistromathics";
            user1.Age = 777;

            dynamic user2 = new ExpandoObject();
            user2.Name = "Wowbagger";
            user2.Password = "teatime";
            user2.Age = int.MaxValue;

            var users = new[] { user1, user2 };

            IList<dynamic> actuals = _db.Users.Insert(users).ToList();

            Assert.Equal(2, actuals.Count);
            Assert.NotEqual(0, actuals[0].Id);
            Assert.Equal("Slartibartfast", actuals[0].Name);
            Assert.Equal("bistromathics", actuals[0].Password);
            Assert.Equal(777, actuals[0].Age);

            Assert.NotEqual(0, actuals[1].Id);
            Assert.Equal("Wowbagger", actuals[1].Name);
            Assert.Equal("teatime", actuals[1].Password);
            Assert.Equal(int.MaxValue, actuals[1].Age);
        }
    }
}