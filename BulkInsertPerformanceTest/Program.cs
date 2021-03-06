﻿using System;
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
using Simple.Data.Firebird.Test;

namespace BulkInsertPerformanceTest
{
    class Program
    {
        static void Main(string[] args)
        {
            RunBenchmark();
        }

        private static void RunNumericBulkInsert()
        {
            new InsertBenchmarks().NumericBulkInsert();
        }
        private static void RunStandardBulkInsert()
        {
            new InsertBenchmarks().StandardBulkInsert();
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
        private int testObjectCount = 1000000;
        private int testObject2Count = 100000;
        private int testObject3Count = 100000;


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
            BulkInsertConfiguration.UseFasterUnsafeBulkInsertMethod = true;
            db.Test_Table.Insert(TestObjects(testObjectCount));
            db.Test_Table2.Insert(TestObjects2(testObject2Count));
            BulkInsertConfiguration.UseFasterUnsafeBulkInsertMethod = false;
        }

        //[Benchmark]
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

        //[Benchmark]
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

        [Benchmark]
        internal void NumericBulkInsert()
        {
            db.Test_Table3.Insert(TestObjects3(testObject3Count));
        }

        [Benchmark]
        internal void UnsafeNumericBulkInsert()
        {
            BulkInsertConfiguration.UseFasterUnsafeBulkInsertMethod = true;
            db.Test_Table3.Insert(TestObjects3(testObject3Count));
            BulkInsertConfiguration.UseFasterUnsafeBulkInsertMethod = false;
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


        static IEnumerable<TestObject3> TestObjects3(int count)
        {
            var testObject = new TestObject3
            {
                Field1 = 1 << 0,
                Field2 = 1 << 2,
                Field3 = 1 << 4,
                Field4 = 1 << 6,
                Field5 = 1 << 8,
                Field6 = 1 << 10,
                Field7 = 1 << 12,
                Field8 = 1 << 14,
                Field9 = 1 << 16,
                Field10 = 1 << 18
            };

            for (int i = 0; i < count; i++)
            {
                yield return testObject;
            }
        }
    }
}
