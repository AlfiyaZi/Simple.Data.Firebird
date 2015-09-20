using System;
using Xunit;

namespace Simple.Data.Firebird.Test
{
    public class NaturalJoinTest : IClassFixture<DbHelper>
    {
        private dynamic _db;

        public NaturalJoinTest(DbHelper helper)
        {
            _db = helper.OpenDefault();
        }

        [Fact]
        public void CustomerDotOrdersDotOrderDateShouldReturnOneRow()
        {
            var row = _db.Customers.Find(_db.Customers.Orders.OrderDate == new DateTime(2010, 10, 10));
            Assert.NotNull(row);
            Assert.Equal("Test", row.Name);
        }

        [Fact]
        public void CustomerDotOrdersDotOrderItemsDotItemDotNameShouldReturnOneRow()
        {
            var customer = _db.Customers.Find(_db.Customers.Orders.OrderItems.Item.Name == "Widget");
            Assert.NotNull(customer);
            Assert.Equal("Test", customer.Name);
            foreach (var order in customer.Orders)
            {
                Assert.Equal(1, order.Id);
            }
        }
    }
}