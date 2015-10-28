using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet;
using BulkInsertPerformanceTest.Properties;
using FirebirdSql.Data.FirebirdClient;
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
            RunBenchmark();
        }

        private static void RunLoopInsertWithCommonTransaction()
        {
            new InsertBenchmarks().LoopInsertWithCommonTransaction();
        }

        private static void RunLoopInsert()
        {
            new InsertBenchmarks().LoopInsert();
        }

        private static void RunBenchmark()
        {
            new BenchmarkRunner().RunCompetition(new InsertBenchmarks());
        }
    }


    public class InsertBenchmarks
    {
        private dynamic db;
        private int testObjectCount = 1000;
        private int testObject2Count = 100;

        public InsertBenchmarks()
        {
            DbHelper.DbCreationScript = Resources.create_db;
            db = new DbHelper().OpenDefault();
        }

        [Benchmark]
        public void StandardBulkInsert()
        {
            db.Test_Table.Insert(TestObjects(testObjectCount));
            db.Test_Table2.Insert(TestObjects2(testObject2Count));
        }

        [Benchmark]
        public void UnsafeBulkInsert()
        {
            BulkInserterConfiguration.UseFasterUnsafeBulkInsertMethod = true;
            db.Test_Table.Insert(TestObjects(testObjectCount));
            db.Test_Table2.Insert(TestObjects2(testObject2Count));
            BulkInserterConfiguration.UseFasterUnsafeBulkInsertMethod = false;
        }

        [Benchmark]
        public void LoopInsert()
        {
            foreach (var testObject in TestObjects(testObjectCount))
            {
                db.Test_Table.Insert(testObject);
            }
            foreach (var testObject in TestObjects2(testObjectCount))
            {
                db.Test_Table2.Insert(testObject);
            }
        }

        [Benchmark]
        public void LoopInsertWithCommonTransaction()
        {
            using (var tx = db.BeginTransaction())
            {
                foreach (var testObject in TestObjects(testObjectCount))
                {
                    tx.Test_Table.Insert(testObject);
                }
                foreach (var testObject in TestObjects2(testObjectCount))
                {
                    tx.Test_Table2.Insert(testObject);
                }

                tx.Commit();
            }
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
