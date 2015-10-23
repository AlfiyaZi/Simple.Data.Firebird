using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using BulkInsertPerformanceTest.Properties;
using Simple.Data.Firebird.Test;

namespace BulkInsertPerformanceTest
{
    class Program
    {
        static void Main(string[] args)
        {
            DbHelper.DbCreationScript = Resources.create_db;
            var db = new DbHelper().OpenDefault();

            int objectCount = 1000000;

            var s = Stopwatch.StartNew();
            db.Test_Table.Insert(TestObjects(objectCount));
            s.Stop();
            Console.WriteLine(s.Elapsed);
            Console.ReadKey();

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
    }
}
