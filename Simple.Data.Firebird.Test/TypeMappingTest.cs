using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xunit;
// ReSharper disable InconsistentNaming

namespace Simple.Data.Firebird.Test
{
    public class TypeMappingTest
    {
        public class TypeMapping_NaturalNumbersTest
        {
            public List<dynamic> NaturalNumbers { get; set; }

            public TypeMapping_NaturalNumbersTest()
            {
                var db = Database.Open();

                NaturalNumbers = db.TypesNumbersNatural.All().ToList();
            }

            [Fact]
            public void NaturalNumbers_RowWithOnlyZeros_ValuesAreZeros()
            {
                var naturalNumbersWithZeros = NaturalNumbers.FirstOrDefault(el => el.Id == 0);

                Assert.NotNull(naturalNumbersWithZeros);
                Assert.Equal(0, naturalNumbersWithZeros.TestSmallint);
                Assert.Equal(0, naturalNumbersWithZeros.TestInteger);
                Assert.Equal(0, naturalNumbersWithZeros.TestBigint);
            }

            [Fact]
            public void NaturalNumbers_RowWithOnlyNulls_ValuesAreNulls()
            {
                var naturalNumbersWithNulls = NaturalNumbers.FirstOrDefault(el => el.Id == 1);

                Assert.NotNull(naturalNumbersWithNulls);
                Assert.Equal(null, naturalNumbersWithNulls.TestSmallint);
                Assert.Equal(null, naturalNumbersWithNulls.TestInteger);
                Assert.Equal(null, naturalNumbersWithNulls.TestBigint);
            }

            [Fact]
            public void NaturalNumbers_RowWithMaxValues_ValuesAreEqualToMax()
            {
                var naturalNumbersWithMaxValue = NaturalNumbers.FirstOrDefault(el => el.Id == 2);

                Assert.NotNull(naturalNumbersWithMaxValue);
                Assert.Equal(32767, naturalNumbersWithMaxValue.TestSmallint);
                Assert.Equal(2147483647, naturalNumbersWithMaxValue.TestInteger);
                Assert.Equal(9223372036854775807, naturalNumbersWithMaxValue.TestBigint);
            }

            [Fact]
            public void NaturalNumbers_RowWithNumbers_MapToCorrectClrTypes()
            {
                var naturalNumbersWithZeros = NaturalNumbers.FirstOrDefault(el => el.Id == 0);

                Assert.NotNull(naturalNumbersWithZeros);
                Assert.Equal(typeof (Int16), naturalNumbersWithZeros.TestSmallint.GetType());
                Assert.Equal(typeof (Int32), naturalNumbersWithZeros.TestInteger.GetType());
                Assert.Equal(typeof (Int64), naturalNumbersWithZeros.TestBigint.GetType());
            }
        }

        public class TypeMapping_RealNumbersTest
        {
            public List<dynamic> RealNumbers { get; set; }

            public TypeMapping_RealNumbersTest()
            {
                var db = Database.Open();

                RealNumbers = db.TypesNumbersReal.All().ToList();
            }

            [Fact]
            public void RealNumbers_RowWithOnlyZeros_ValuesAreZeros()
            {
                var realNumbersWithZeros = RealNumbers.FirstOrDefault(el => el.Id == 0);

                Assert.NotNull(realNumbersWithZeros);
                Assert.Equal(0, realNumbersWithZeros.TestFloat);
                Assert.Equal(0, realNumbersWithZeros.TestDouble);
                Assert.Equal(0, realNumbersWithZeros.TestNumericS);
                Assert.Equal(0, realNumbersWithZeros.TestNumeric);
                Assert.Equal(0, realNumbersWithZeros.TestNumericB);
                Assert.Equal(0, realNumbersWithZeros.TestDecimalS);
                Assert.Equal(0, realNumbersWithZeros.TestDecimal);
                Assert.Equal(0, realNumbersWithZeros.TestDecimalB);
            }

            [Fact]
            public void RealNumbers_RowWithOnlyNulls_ValuesAreNulls()
            {
                var realNumbersWithNulls = RealNumbers.FirstOrDefault(el => el.Id == 1);

                Assert.NotNull(realNumbersWithNulls);
                Assert.Equal(null, realNumbersWithNulls.TestFloat);
                Assert.Equal(null, realNumbersWithNulls.TestDouble);
                Assert.Equal(null, realNumbersWithNulls.TestNumericS);
                Assert.Equal(null, realNumbersWithNulls.TestNumeric);
                Assert.Equal(null, realNumbersWithNulls.TestNumericB);
                Assert.Equal(null, realNumbersWithNulls.TestDecimalS);
                Assert.Equal(null, realNumbersWithNulls.TestDecimal);
                Assert.Equal(null, realNumbersWithNulls.TestDecimalB);
            }

            [Fact]
            public void RealNumbers_RowWithMaxValues_ValuesAreEqualToMax()
            {
                var realNumbersWithMaxValue = RealNumbers.FirstOrDefault(el => el.Id == 2);

                Assert.NotNull(realNumbersWithMaxValue);
                Assert.Equal(realNumbersWithMaxValue.TestFloat, 3.39999995214436E38f);
                Assert.Equal(realNumbersWithMaxValue.TestDouble, 1.7E308);
                Assert.Equal(realNumbersWithMaxValue.TestNumericS, 327.67m);
                Assert.Equal(realNumbersWithMaxValue.TestNumeric, 214.7483647m);
                Assert.Equal(realNumbersWithMaxValue.TestNumericB, 92233720368.54775807m);
                Assert.Equal(realNumbersWithMaxValue.TestDecimalS, 999.99m);
                Assert.Equal(realNumbersWithMaxValue.TestDecimal, 214.7483647m);
                Assert.Equal(realNumbersWithMaxValue.TestDecimalB, 92233720368.54775807m);
            }

            [Fact]
            public void RealNumbers_RowWith_0_1_Values_RoundingErrorsShouldNotHappen()
            {
                var realNumbersWith_0_1_Values = RealNumbers.FirstOrDefault(el => el.Id == 3);

                Assert.NotNull(realNumbersWith_0_1_Values);
                Assert.Equal(0.1f, realNumbersWith_0_1_Values.TestFloat);
                Assert.Equal(0.1, realNumbersWith_0_1_Values.TestDouble);
                Assert.Equal(0.1m, realNumbersWith_0_1_Values.TestNumericS);
                Assert.Equal(0.1m, realNumbersWith_0_1_Values.TestNumeric);
                Assert.Equal(0.1m, realNumbersWith_0_1_Values.TestNumericB);
                Assert.Equal(0.1m, realNumbersWith_0_1_Values.TestDecimalS);
                Assert.Equal(0.1m, realNumbersWith_0_1_Values.TestDecimal);
                Assert.Equal(0.1m, realNumbersWith_0_1_Values.TestDecimalB);
            }

            [Fact]
            public void RealNumbers_RowWithNumbers_MapToCorrectClrTypes()
            {
                var realNumbersWithZeros = RealNumbers.FirstOrDefault(el => el.Id == 0);

                Assert.NotNull(realNumbersWithZeros);
                Assert.Equal(typeof(Single), realNumbersWithZeros.TestFloat.GetType());
                Assert.Equal(typeof(Double), realNumbersWithZeros.TestDouble.GetType());
                Assert.Equal(typeof(Decimal), realNumbersWithZeros.TestNumericS.GetType());
                Assert.Equal(typeof(Decimal), realNumbersWithZeros.TestNumeric.GetType());
                Assert.Equal(typeof(Decimal), realNumbersWithZeros.TestNumericB.GetType());
                Assert.Equal(typeof(Decimal), realNumbersWithZeros.TestDecimalS.GetType());
                Assert.Equal(typeof(Decimal), realNumbersWithZeros.TestDecimal.GetType());
                Assert.Equal(typeof(Decimal), realNumbersWithZeros.TestDecimalB.GetType());
            }
        }

        public class TypeMapping_DateTimeTest
        {
            public List<dynamic> DateTimes { get; set; }

            public TypeMapping_DateTimeTest()
            {
                var db = Database.Open();

                DateTimes = db.TypesDatetime.All().ToList();
            }

            [Fact]
            public void DateTime_RowWitMinValues_ValuesAreEqualToMin()
            {
                var dateTimesWithMinValue = DateTimes.FirstOrDefault(el => el.Id == 0);

                Assert.NotNull(dateTimesWithMinValue);
                Assert.Equal(DateTime.MinValue, dateTimesWithMinValue.TestDate);
                Assert.Equal(new TimeSpan(0,0,0), dateTimesWithMinValue.TestTime);
                Assert.Equal(DateTime.MinValue, dateTimesWithMinValue.TestTimestamp);
            }

            [Fact]
            public void DateTime_RowWitMaxValues_ValuesAreEqualToMax()
            {
                var dateTimesWithMaxValue = DateTimes.FirstOrDefault(el => el.Id == 1);

                Assert.NotNull(dateTimesWithMaxValue);
                Assert.Equal(new DateTime(9999,12,31), dateTimesWithMaxValue.TestDate);
                Assert.Equal(new TimeSpan(23, 59, 59), dateTimesWithMaxValue.TestTime);
                Assert.Equal(new DateTime(9999, 12, 31, 23, 59, 59), dateTimesWithMaxValue.TestTimestamp);
            }

            [Fact]
            public void DateTime_RowWithNullValues_ValuesAreNull()
            {
                var dateTimesWithMaxValue = DateTimes.FirstOrDefault(el => el.Id == 2);

                Assert.NotNull(dateTimesWithMaxValue);
                Assert.Equal(null, dateTimesWithMaxValue.TestDate);
                Assert.Equal(null, dateTimesWithMaxValue.TestTime);
                Assert.Equal(null, dateTimesWithMaxValue.TestTimestamp);
            }
        }

        public class TypeMapping_TextsTest
        {
            public List<dynamic> Texts { get; set; }

            public TypeMapping_TextsTest()
            {
                var db = Database.Open();

                Texts = db.TypesText.All().ToList();
            }

            [Fact]
            public void Texts_NotEmptyRow_ValuesAreEqualToExpected()
            {
                var nonEmptyTexts = Texts.FirstOrDefault(el => el.Id == 0);

                Assert.NotNull(nonEmptyTexts);
                Assert.Equal("123456789 ", nonEmptyTexts.TestChar);
                Assert.Equal("123456789", nonEmptyTexts.TestVarchar);
            }

            [Fact]
            public void Texts_EmptyRow_ValuesNull()
            {
                var emptyTexts = Texts.FirstOrDefault(el => el.Id == 1);

                Assert.NotNull(emptyTexts);
                Assert.Equal(null, emptyTexts.TestChar);
                Assert.Equal(null, emptyTexts.TestVarchar);
            }
        }

        public class TypeMapping_BlobTest
        {
            public List<dynamic> Blobs { get; set; }

            public TypeMapping_BlobTest()
            {
                var db = Database.Open();

                Blobs = db.TypesBlob.All().ToList();
            }

            [Fact]
            public void Blobs_EmptyRow_IsNull()
            {
                var emptyBlobs = Blobs.FirstOrDefault(el => el.Id == 0);

                Assert.NotNull(emptyBlobs);
                Assert.Equal(null, emptyBlobs.TestBlobBinary);
                Assert.Equal(null, emptyBlobs.TestBlobText);
            }

            [Fact]
            public void Blobs_FilledText_IsExpectedString()
            {
                var emptyBlobs = Blobs.FirstOrDefault(el => el.Id == 1);

                Assert.NotNull(emptyBlobs);
                Assert.Equal(typeof(String), emptyBlobs.TestBlobText.GetType());
                Assert.Equal("1234567890", emptyBlobs.TestBlobText);
            }

            [Fact]
            public void Blobs_FilledBinary_IsExpectedValue()
            {
                var emptyBlobs = Blobs.FirstOrDefault(el => el.Id == 2);

                Assert.NotNull(emptyBlobs);
                Assert.Equal(typeof(byte[]), emptyBlobs.TestBlobBinary.GetType());
                Assert.True(emptyBlobs.TestBlobBinary.Length == 1);
                Assert.Equal(49, emptyBlobs.TestBlobBinary[0]);     // value for 1
            }
        }
    }
}
