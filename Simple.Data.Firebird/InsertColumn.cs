using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using Simple.Data.Ado.Schema;

namespace Simple.Data.Firebird
{
    internal class InsertColumn
    {
        Dictionary<Type, Func<string>> TypeValueToSql;

        public string Name { get; set; }
        public object Value { get; set; }
        public string ParameterName { get; set; }
        public FbColumn Column { get; set; }

        public InsertColumn()
        {
            TypeValueToSql = new Dictionary<Type, Func<string>>
            {
                {typeof(string), () => String.Format("'{0}'",Value.ToString().Replace("'", "''"))},
                {typeof(bool), () => ((bool) Value) ? "CAST(1 AS SMALLINT)" : "CAST(0 AS SMALLINT)"},
                {typeof(short), () => ((short) Value).ToString()},
                {typeof(int), () => ((int) Value).ToString()},    // using explict cast with short/int/long to avoid problems with enums
                {typeof(long), () => ((long) Value).ToString()},
                {typeof(float), () => ((float)Value).ToString(CultureInfo.InvariantCulture)},
                {typeof(double), () => ((double)Value).ToString(CultureInfo.InvariantCulture)},
                {typeof(decimal), () => ((decimal)Value).ToString(CultureInfo.InvariantCulture)},
                {typeof(DateTime), () => String.Format("'{0}'", Column.DbType == DbType.Date ? ((DateTime)Value).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) : ((DateTime)Value).ToString("yyyy-MM-dd HH:mm:ss.ffff", CultureInfo.InvariantCulture))},
                {typeof(TimeSpan), () => String.Format("'{0}'", ((TimeSpan)Value).ToString("HH:mm:ss.ffff", CultureInfo.InvariantCulture))}
            };
        }

        public string ValueToSql()
        {
            if (Value == null) return "null";
            var valueType = Value.GetType();
            if (TypeValueToSql.ContainsKey(valueType)) return TypeValueToSql[valueType]();
            if (valueType.IsEnum)
            {
                var underlyingType = Enum.GetUnderlyingType(valueType);
                if (TypeValueToSql.ContainsKey(underlyingType)) return TypeValueToSql[underlyingType]();
            }
            //if no type match was found treat it same way as string
            return TypeValueToSql[typeof (string)]();
        }

    }

      
}