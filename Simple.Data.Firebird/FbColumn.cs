using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado.Schema;

namespace Simple.Data.Firebird
{
    class FbColumn : Column
    {
        private readonly int _precision;
        internal int Size { get; private set; }
        public TypeEntry Type { get; private set; }

        public FbColumn(string actualName, Table table) : base(actualName, table)
        {
        }

        public FbColumn(string actualName, Table table, DbType dbType) : base(actualName, table, dbType)
        {
        }

        public FbColumn(string actualName, Table table, bool isIdentity) : base(actualName, table, isIdentity)
        {
        }

        public FbColumn(string actualName, Table table, bool isIdentity, DbType dbType, int maxLength) : base(actualName, table, isIdentity, dbType, maxLength)
        {
        }

        public FbColumn(string actualName, Table table, bool isIdentity, int maxLength, int precision, int size, TypeEntry type)
            : base(actualName, table, isIdentity, type.DbType, maxLength)
        {
            _precision = precision;
            Type = type;
            Size = size;
        }

        public string TypeSql
        {
            get { return Type.FbTypeName + LengthPrecisionSql(); }
        }

        public string NameTypeSql
        {
            get { return QuotedName + " " + Type.FbTypeName + LengthPrecisionSql(); }
        }

        private string LengthPrecisionSql()
        {
            if (Type.WithPrecision) return String.Format("({0},{1})", MaxLength, _precision);
            if (Type.WithLength) return String.Format("({0})", MaxLength);
            return "";
        }
    }
}
