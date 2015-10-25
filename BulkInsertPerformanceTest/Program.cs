using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using BulkInsertPerformanceTest.Properties;
using Simple.Data;
using Simple.Data.Firebird;
using Simple.Data.Firebird.BulkInsert;
using Simple.Data.Firebird.Test;

namespace BulkInsertPerformanceTest
{
    class Program
    {
        static void Main(string[] args)
        {
            DbHelper.DbCreationScript = Resources.create_db;
            BulkInserterConfiguration.UseFasterUnsafeBulkInsertMethod = true;

            var db = new DbHelper().OpenDefault();

            var s = Stopwatch.StartNew();

            db.Test_Table.Insert(TestObjects(100000));
            db.Test_Table2.Insert(TestObjects2(10000));
            s.Stop();
            Console.WriteLine(s.Elapsed);
            //Console.ReadKey();

        }


        static IEnumerable<TestObject> TestObjects(int count)
        {
            string testText = "123456789x123456789x123456789x";

            var testObject = new TestObject
            {
                Field1 = testText,
                Field2 = testText,
                Field3 = testText
            };

            for (int i = 0; i < count; i++)
            {
                yield return testObject;
            }
        }

        static IEnumerable<TestObject2> TestObjects2(int count)
        {
            string testText = "123456789x";

            var testObject = new TestObject2
            {
                Field1 = testText,
                Field2 = testText,
                Field3 = testText,
                Field4 = testText,
                Field5 = testText,
                Field6 = testText,
                Field7 = testText,
                Field8 = testText,
                Field9 = testText,
                Field10 = testText,
                Field11 = testText,
                Field12 = testText,
                Field13 = testText,
                Field14 = testText,
                Field15 = testText,
                Field16 = testText,
                Field17 = testText,
                Field18 = testText,
                Field19 = testText,
                Field20 = testText,
                Field21 = testText,
                Field22 = testText,
                Field23 = testText,
                Field24 = testText,
                Field25 = testText,
                Field26 = testText,
                Field27 = testText,
                Field28 = testText,
                Field29 = testText,
                Field30 = testText
            };

            for (int i = 0; i < count; i++)
            {
                yield return testObject;
            }
        } 
    }
}
