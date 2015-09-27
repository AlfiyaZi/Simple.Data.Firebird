using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Simple.Data.Ado.Schema;

namespace Simple.Data.Firebird
{
    public class FbProcedureParameter : Parameter
    {
        public bool Optional { get; private set; }

        public int Position { get; private set; }

        public FbProcedureParameter(string name, Type type, ParameterDirection direction, int position, bool optional) : base(name, type, direction)
        {
            Position = position;
            Optional = optional;
        }

        public FbProcedureParameter(string name, Type type, ParameterDirection direction, DbType dbtype, int size, int position, bool optional)
            : base(name, type, direction, dbtype, size)
        {
            Position = position;
            Optional = optional;
        }
    }
}
