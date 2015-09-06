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
            if (typeId == "16" && (subTypeId == "0" || subTypeId == null)) return GetTypeEntry("bigint");   //on some system tables bigint fields don't have subTypeId
            if (typeId == "16" && subTypeId == "1") return GetTypeEntry("numeric");
            if (typeId == "16" && subTypeId == "2") return GetTypeEntry("decimal");
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
                {"smallint", new TypeEntry("smallint", DbType.Int16, FbDbType.SmallInt, typeof (Int16))},
                {"integer", new TypeEntry("integer", DbType.Int32, FbDbType.Integer, typeof (Int32))},
                {"char", new TypeEntry("char", DbType.StringFixedLength, FbDbType.Char, typeof (String))},
                {"varchar", new TypeEntry("varchar", DbType.String, FbDbType.VarChar, typeof (String))},
                {"float", new TypeEntry("float", DbType.Single, FbDbType.Float, typeof (Single))},
                {"numeric s", new TypeEntry("numeric s", DbType.Double, FbDbType.Numeric, typeof (Decimal))},
                {"decimal s", new TypeEntry("decimal s", DbType.Double, FbDbType.Decimal, typeof (Decimal))},
                {"double precision", new TypeEntry("double precision", DbType.Double, FbDbType.Double, typeof (Double))},
                {"numeric", new TypeEntry("numeric", DbType.Decimal, FbDbType.Numeric, typeof (Decimal))},
                {"decimal", new TypeEntry("decimal", DbType.Decimal, FbDbType.Decimal, typeof (Decimal))},
                {"timestamp", new TypeEntry("timestamp", DbType.DateTime, FbDbType.TimeStamp, typeof (DateTime))},
                {"date", new TypeEntry("date", DbType.Date, FbDbType.Date, typeof (DateTime))},
                {"time", new TypeEntry("time", DbType.Time, FbDbType.Time, typeof (TimeSpan))},
                {"bigint", new TypeEntry("bigint", DbType.Int64, FbDbType.BigInt, typeof (Int64))},
                {"blob sub_type binary", new TypeEntry("blob sub_type binary", DbType.Binary, FbDbType.Binary, typeof (byte[]))},
                {"blob sub_type text", new TypeEntry("blob sub_type text",DbType.Binary, FbDbType.Binary, typeof (String))},
                {"blob sub_type blr", new TypeEntry("blob sub_type blr", DbType.Binary, FbDbType.Binary, typeof (byte[]))},
            };


        private static readonly Dictionary<string, string> FbTypeIdToTypeName
            = new Dictionary<string, string>
            {
                {"7", "smallint"},
                {"8", ""}
            };
 
    }

    public class TypeEntry
    {
        public string FbTypeName { get; private set; }
        public DbType DbType { get; private set; }
        public FbDbType FbDbType { get; private set; }
        public Type ClrType { get; private set; }

        public TypeEntry(string fbTypeName, DbType dbType, FbDbType fbDbType, Type clrType)
        {
            FbTypeName = fbTypeName;
            DbType = dbType;
            FbDbType = fbDbType;
            ClrType = clrType;
        }
    }
}