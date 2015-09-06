using System;
using System.ComponentModel.Composition;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado;
using Simple.Data.Ado.Schema;

namespace Simple.Data.Firebird
{
    [Export("FirebirdSql.Data.FirebirdClient", typeof(IConnectionProvider))]
    public class FbConnectionProvider : IConnectionProvider
    {
        public void SetConnectionString(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public IDbConnection CreateConnection()
        {
            return new FbConnection(ConnectionString);
        }

        public ISchemaProvider GetSchemaProvider()
        {
            return new FbSchemaProvider(this);
        }

        public string GetIdentityFunction()
        {
            // There is no global identity function in Firebird
            throw new InvalidOperationException("Firebird does not have a global identity function");
        }

        public IProcedureExecutor GetProcedureExecutor(AdoAdapter adapter, ObjectName procedureName)
        {
            return new FbProcedureExecutor(adapter, procedureName);
        }

        public string ConnectionString { get; private set; }

        public bool SupportsCompoundStatements { get { return false; } }

        public bool SupportsStoredProcedures { get { return true; } }
    }
}
