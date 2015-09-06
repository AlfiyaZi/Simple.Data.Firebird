using System;
using System.Collections.Generic;
using System.Data;
using Simple.Data.Ado;

namespace Simple.Data.Firebird
{
    public class FbProcedureExecutor : IProcedureExecutor
    {
        public AdoAdapter Adapter { get; set; }
        public ObjectName ProcedureName { get; set; }

        public FbProcedureExecutor(AdoAdapter adapter, ObjectName procedureName)
        {
            Adapter = adapter;
            ProcedureName = procedureName;
        }

        public IEnumerable<IEnumerable<IDictionary<string, object>>> Execute(IDictionary<string, object> suppliedParameters)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IEnumerable<IDictionary<string, object>>> Execute(IDictionary<string, object> suppliedParameters, IDbTransaction transaction)
        {
            throw new NotImplementedException();
        }
    }
}