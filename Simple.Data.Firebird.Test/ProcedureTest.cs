using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Simple.Data.Firebird.Test
{
    public class ProcedureTest : IClassFixture<DbHelper>
    {
        private dynamic _db;

        public ProcedureTest(DbHelper helper)
        {
            _db = helper.OpenDefault();
        }

        [Fact]
        public void TestNoReturn()
        {
            var result = _db.TestNoReturn();
            Assert.False(result.NextResult());
        }

        [Fact]
        public void TestNoReturnInputParameter()
        {
            var result = _db.TestNoReturnInputParameter(1);
            Assert.False(result.NextResult());
        }

        [Fact]
        public void TestNoReturnAllOptionalParameters()
        {
            var result = _db.TestNoReturnOptInputParams(1,2,3,4);
            Assert.False(result.NextResult());
        }

        [Fact]
        public void TestNoReturnLastOptionalParameterSkipped()
        {
            var result = _db.TestNoReturnOptInputParams(1,2,3);
            Assert.False(result.NextResult());
        }

        [Fact]
        public void TestNoReturnNotLastOptionalParameterSkipped()
        {
            var ex = Record.Exception(() => _db.TestNoReturnOptInputParams(id: 1, opt: 1, opt2: 2));

            Assert.IsType<SimpleDataException>(ex);
        }

        [Fact]
        public void TestNoReturnInsertToTable()
        {
            _db.TestNoReturnOptInputParams(1);

            var tableResult = _db.TestNoReturnOptInputVals.FindById(1);
            Assert.Equal(1, tableResult.Id);
            Assert.Equal(null, tableResult.Opt);
            Assert.Equal(1, tableResult.Opt1);
            Assert.Equal(2, tableResult.Opt2);
        }

        [Fact]
        public void TestReturnNoInputParams()
        {
            var result = _db.TestReturnNoInputParams().First();

            Assert.Equal(1, result.Val);
            Assert.Equal(null, result.NullVal);
        }

        [Fact]
        public void TestReturnInputOutputParams()
        {
            var result = _db.TestReturnInOutParams(1).ToList();

            Assert.Equal(1, result[0].Val);
            Assert.Equal(2, result[1].Val);
        }
    }
}
