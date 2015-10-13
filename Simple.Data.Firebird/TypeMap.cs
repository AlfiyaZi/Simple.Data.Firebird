using System;
using System.Collections.Generic;
using System.Data;
using FirebirdSql.Data.FirebirdClient;

namespace Simple.Data.Firebird
{
    public class TypeMap
    {
        public static TypeEntry GetTypeEntry(string dataTypeName)
        {
            if (!FbTypeToTypeEntry.ContainsKey(dataTypeName))
                throw new ArgumentException(String.Format("Unknown data type name: {0}", dataTypeName));

            return FbTypeToTypeEntry[dataTypeName];
        }

        /// <exception cref="ArgumentException">Throws when typeId and subTypeId do not match any expected type.</exception>
        public static TypeEntry GetTypeEntry(string typeId, string subTypeId)
        {
            if (typeId == "7" && subTypeId == "0") return GetTypeEntry("smallint");
            if (typeId == "7" && subTypeId == "1") return GetTypeEntry("numeric s");
            if (typeId == "7" && subTypeId == "2") return GetTypeEntry("decimal s");
            if (typeId == "8" && subTypeId == "0") return GetTypeEntry("integer");
            if (typeId == "8" && subTypeId == "1") return GetTypeEntry("numeric s");
            if (typeId == "8" && subTypeId == "2") return GetTypeEntry("decimal s");                    
            if (typeId == "10") return GetTypeEntry("float");
            if (typeId == "12") return GetTypeEntry("date");
            if (typeId == "13") return GetTypeEntry("time");
            if (typeId == "14") return GetTypeEntry("char");
            if (typeId == "16" && subTypeId == "1") return GetTypeEntry("numeric");
            if (typeId == "16" && subTypeId == "2") return GetTypeEntry("decimal");
            if (typeId == "16") return GetTypeEntry("bigint");   //on some system tables bigint fields don't have subTypeId
            if (typeId == "27") return GetTypeEntry("double precision");
            if (typeId == "35") return GetTypeEntry("timestamp");
            if (typeId == "37") return GetTypeEntry("varchar");
            if (typeId == "261" && subTypeId == "0") return GetTypeEntry("blob sub_type binary");
            if (typeId == "261" && subTypeId == "1") return GetTypeEntry("blob sub_type text");
            if (typeId == "261" && subTypeId == "2") return GetTypeEntry("blob sub_type blr");
            if (typeId == "261") return GetTypeEntry("blob sub_type binary");       // we'll just cast  other blob subtypes used in system tables to binary

            throw new ArgumentException(String.Format("Unknown data type name for typeId:{0}, subTypeId:{1}", typeId, subTypeId));
        }


        private static readonly Dictionary<string, TypeEntry> FbTypeToTypeEntry =
            new Dictionary<string, TypeEntry>
            {
                {"smallint", new TypeEntry("smallint", DbType.Int16, FbDbType.SmallInt, typeof (Int16), true, true)},
                {"integer", new TypeEntry("integer", DbType.Int32, FbDbType.Integer, typeof (Int32), false, false)},
                {"char", new TypeEntry("char", DbType.StringFixedLength, FbDbType.Char, typeof (String), true, false)},
                {"varchar", new TypeEntry("varchar", DbType.String, FbDbType.VarChar, typeof (String), true, false)},
                {"float", new TypeEntry("float", DbType.Single, FbDbType.Float, typeof (Single), false, false)},
                {"numeric s", new TypeEntry("numeric", DbType.Double, FbDbType.Numeric, typeof (Decimal), true, true)},
                {"decimal s", new TypeEntry("decimal", DbType.Double, FbDbType.Decimal, typeof (Decimal), true, true)},
                {"double precision", new TypeEntry("double precision", DbType.Double, FbDbType.Double, typeof (Double), false, false)},
                {"numeric", new TypeEntry("numeric", DbType.Decimal, FbDbType.Numeric, typeof (Decimal), true, true)},
                {"decimal", new TypeEntry("decimal", DbType.Decimal, FbDbType.Decimal, typeof (Decimal), true, true)},
                {"timestamp", new TypeEntry("timestamp", DbType.DateTime, FbDbType.TimeStamp, typeof (DateTime), false, false)},
                {"date", new TypeEntry("date", DbType.Date, FbDbType.Date, typeof (DateTime), false, false)},
                {"time", new TypeEntry("time", DbType.Time, FbDbType.Time, typeof (TimeSpan), false, false)},
                {"bigint", new TypeEntry("bigint", DbType.Int64, FbDbType.BigInt, typeof (Int64), false, false)},
                {"blob sub_type binary", new TypeEntry("blob sub_type binary", DbType.Binary, FbDbType.Binary, typeof (byte[]), false, false)},
                {"blob sub_type text", new TypeEntry("blob sub_type text",DbType.Binary, FbDbType.Binary, typeof (String), false, false)},
                {"blob sub_type blr", new TypeEntry("blob sub_type blr", DbType.Binary, FbDbType.Binary, typeof (byte[]), false, false)},
            };
 
    }

    public class TypeEntry
    {
        public string FbTypeName { get; private set; }
        public DbType DbType { get; private set; }
        public FbDbType FbDbType { get; private set; }
        public Type ClrType { get; private set; }
        public bool WithLength { get; set; }
        public bool WithPrecision { get; set; }

        public TypeEntry(string fbTypeName, DbType dbType, FbDbType fbDbType, Type clrType, bool withLength, bool withPrecision)
        {
            FbTypeName = fbTypeName;
            DbType = dbType;
            FbDbType = fbDbType;
            ClrType = clrType;
            WithLength = withLength;
            WithPrecision = withPrecision;
        }
    }
}