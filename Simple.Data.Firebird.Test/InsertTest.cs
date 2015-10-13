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

        internal class PersonWithEnum
        {
            public int? Id { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
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
            var person = new PersonWithEnum
            {
                Name = "Zaphod",
                Surname = "zarquon",
                Age = AgeDescription.Old
            };

            PersonWithEnum actual = _db.Persons.Insert(person);

            Assert.NotNull(person);
            Assert.NotEqual(0, actual.Id);
            Assert.Equal("Zaphod", actual.Name);
            Assert.Equal("zarquon", actual.Surname);
            Assert.Equal(80, (int)actual.Age);
            Assert.Equal(AgeDescription.Old, actual.Age);
        }

        [Fact]
        public void TestInsertManyWithStaticTypeObjectWithEnum()
        {
            var person1 = new PersonWithEnum
            {
                Name = "Zaphod",
                Surname = "zarquon",
                Age = AgeDescription.Old
            };
            var person2 = new PersonWithEnum
            {
                Name = "Zaphod2",
                Surname = "zarquon2",
                Age = AgeDescription.Young
            };

            List<PersonWithEnum> actual = _db.Persons.Insert(new[] { person1, person2 }).ToList<PersonWithEnum>();

            Assert.NotNull(actual[0]);
            Assert.NotEqual(0, actual[0].Id);
            Assert.Equal("Zaphod", actual[0].Name);
            Assert.Equal("zarquon", actual[0].Surname);
            Assert.Equal(80, (int)actual[0].Age);
            Assert.Equal(AgeDescription.Old, actual[0].Age);

            Assert.NotNull(actual[1]);
            Assert.NotEqual(0, actual[1].Id);
            Assert.Equal("Zaphod2", actual[1].Name);
            Assert.Equal("zarquon2", actual[1].Surname);
            Assert.Equal(1, (int)actual[1].Age);
            Assert.Equal(AgeDescription.Young, actual[1].Age);
        }

        [Fact]
        public void TestInsertWithObjectWithNullValue()
        {
            var actual = _db.Persons.Insert(Name: "John", Surname: "Doe", MiddleName: null, Age: 42);

            Assert.Equal("John", actual.Name);
            Assert.Equal("Doe", actual.Surname);
            Assert.Equal(42, actual.Age);
            Assert.Null(actual.MiddleName);
        }

        [Fact]
        public void TestInsertWithNamedArguments()
        {
            var person = _db.Persons.Insert(Name: "Ford", Surname: "hoopy", Age: 29);

            Assert.NotNull(person);
            Assert.Equal("Ford", person.Name);
            Assert.Equal("hoopy", person.Surname);
            Assert.Equal(29, person.Age);
        }

        [Fact]
        public void TestInsertWithStaticTypeObject()
        {
            var person = new Person { Name = "Zaphod", Surname = "zarquon", Age = 42 };

            var actual = _db.Persons.Insert(person);

            Assert.NotNull(person);
            Assert.NotEqual(0, actual.Id);
            Assert.Equal("Zaphod", actual.Name);
            Assert.Equal("zarquon", actual.Surname);
            Assert.Equal(42, actual.Age);
        }

        [Fact]
        public void TestMultiInsertWithStaticTypeObjects()
        {
            var persons = new[]
                  {
                    new Person {Name = "Slartibartfast", Surname = "bistromathics", Age = 777},
                    new Person {Name = "Wowbagger", Surname = "teatime", Age = int.MaxValue}
                  };

            IList<Person> actuals = _db.Persons.Insert(persons).ToList<Person>();

            Assert.Equal(2, actuals.Count);
            Assert.NotEqual(0, actuals[0].Id);
            Assert.Equal("Slartibartfast", actuals[0].Name);
            Assert.Equal("bistromathics", actuals[0].Surname);
            Assert.Equal(777, actuals[0].Age);

            Assert.NotEqual(0, actuals[1].Id);
            Assert.Equal("Wowbagger", actuals[1].Name);
            Assert.Equal("teatime", actuals[1].Surname);
            Assert.Equal(int.MaxValue, actuals[1].Age);
        }

        [Fact]
        public void TestInsertWithDynamicTypeObject()
        {
            dynamic person = new ExpandoObject();
            person.Name = "Marvin";
            person.Surname = "diodes";
            person.Age = 42000000;

            var actual = _db.Persons.Insert(person);

            Assert.NotNull(person);
            Assert.Equal("Marvin", actual.Name);
            Assert.Equal("diodes", actual.Surname);
            Assert.Equal(42000000, actual.Age);
        }

        [Fact]
        public void TestMultiInsertWithDynamicTypeObjects()
        {
            dynamic person1 = new ExpandoObject();
            person1.Name = "Slartibartfast";
            person1.Surname = "bistromathics";
            person1.Age = 777;

            dynamic person2 = new ExpandoObject();
            person2.Name = "Wowbagger";
            person2.Surname = "teatime";
            person2.Age = int.MaxValue;

            var persons = new[] { person1, person2 };

            IList<dynamic> actuals = _db.Persons.Insert(persons).ToList();

            Assert.Equal(2, actuals.Count);
            Assert.Equal("Slartibartfast", actuals[0].Name);
            Assert.Equal("bistromathics", actuals[0].Surname);
            Assert.Equal(777, actuals[0].Age);

            Assert.Equal("Wowbagger", actuals[1].Name);
            Assert.Equal("teatime", actuals[1].Surname);
            Assert.Equal(int.MaxValue, actuals[1].Age);
        }

        [Fact]
        public void TestInsertBlobs()
        {
            _db.TypesBlob.Insert(Id: 10, TestBlobText: "123", TestBlobBinary: new byte[] {50});

            var insertedValues = _db.TypesBlob.FindById(10);
            Assert.NotNull(insertedValues);
            Assert.Equal("123", insertedValues.TestBlobText);
            Assert.Equal(new byte[] { 50 }, insertedValues.TestBlobBinary);
        }

        [Fact]
        public void TestInsertWithNonClrObject()
        {
            var actual = _db.Persons.Insert(Name: "John", Surname: new Surname("Doe"), MiddleName: null, Age: 42);

            Assert.Equal("John", actual.Name);
            Assert.Equal("Doe", actual.Surname);
            Assert.Equal(42, actual.Age);
            Assert.Null(actual.MiddleName);
        }

        [Fact]
        public void TestInsertManyWithNonClrObject()
        {
            var actual = _db.Persons.Insert(new[]
            {
                new {Name = "John", Surname = "", Age = 42},
                new {Name = "Lois", Surname = "", Age = 42}
            });

            //Assert.Equal("John", actual.Name);
            //Assert.Equal("Doe", actual.Surname);
            //Assert.Equal(42, actual.Age);
            //Assert.Null(actual.MiddleName);
        }

    public class Surname
    {
        private readonly string _surname;

        public Surname(string surname)
        {
            _surname = surname;
        }

        public override string ToString()
        {
            return _surname;
        }
    }
}
}