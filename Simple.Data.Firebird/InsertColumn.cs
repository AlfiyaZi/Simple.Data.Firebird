using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using Simple.Data.Ado.Schema;

namespace Simple.Data.Firebird
{
    internal class InsertColumn
    {
        public string Name { get; set; }
        public object Value { get; set; }
        public string ParameterName { get; set; }
        public FbColumn Column { get; set; }

        public string ValueToSql()
        {
            if (Value == null) return "null";

            var currentValue = Value;
            var valueType = Value.GetType();
            TypeCode typeCode = Type.GetTypeCode(valueType);

            if (valueType.IsEnum) currentValue = Convert.ChangeType(Value, typeCode);

            switch (typeCode)
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return currentValue.ToString();
                case TypeCode.Boolean:
                    return Convert.ToInt32((bool) currentValue).ToString();
                case TypeCode.Single:
                    return ((float) currentValue).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Double:
                    return ((double) currentValue).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Decimal:
                    return ((decimal) currentValue).ToString(CultureInfo.InvariantCulture);
                case TypeCode.DBNull:
                    return null;
                case TypeCode.DateTime:
                    return DateTimeSql((DateTime) currentValue);
                case TypeCode.String:
                    return StringSql((string) currentValue);
            }
            if (valueType == typeof(TimeSpan))
                return String.Format("'{0}'", ((TimeSpan) currentValue).ToString("HH:mm:ss.ffff", CultureInfo.InvariantCulture));

            //if no type match was found treat it same way as string to avoid sql injection when using custom types
            return StringSql(currentValue.ToString());
        }

        private string StringSql(string value)
        {
            return  String.Format("'{0}'", value.Replace("'", "''"));
        }

        private string DateTimeSql(DateTime dateTime)
        {
            return String.Format("'{0}'",
                Column.DbType == DbType.Date
                    ? dateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                    : dateTime.ToString("yyyy-MM-dd HH:mm:ss.ffff", CultureInfo.InvariantCulture));
        }
    }   
}