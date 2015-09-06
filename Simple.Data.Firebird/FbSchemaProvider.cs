using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado.Schema;
using Simple.Data.Firebird.Properties;

namespace Simple.Data.Firebird
{
    public class FbSchemaProvider : ISchemaProvider
    {
        public FbConnectionProvider ConnectionProvider { get; private set; }

        private Lazy<IEnumerable<Table>> _tables = new Lazy<IEnumerable<Table>>();

        public FbSchemaProvider(FbConnectionProvider connectionProvider)
        {
            ConnectionProvider = connectionProvider;
            _tables = new Lazy<IEnumerable<Table>>(LoadTables);
        }

        public IEnumerable<Table> GetTables()
        {
            return _tables.Value;
        }

        private IEnumerable<Table> LoadTables()
        {
            return SelectToDataTable(Resources.TablesQuery)
            .AsEnumerable()
            .Select(table => new Table(table["table_name"].ToString(), null,
                                       table["is_view"].ToString().Equals("VIEW", StringComparison.InvariantCultureIgnoreCase) ? TableType.View : TableType.Table));
        }

        public IEnumerable<Column> GetColumns(Table table)
        {
            if (table == null) throw new ArgumentNullException("table");

            return SelectToDataTable(String.Format(Resources.ColumnsQuery, table.ActualName))
            .AsEnumerable()
            .Select(columnRow => new Column(columnRow["field_name"].ToString(),
                table,
                false,
                TypeMap.GetTypeEntry(columnRow["field_type"].ToString(),
                    columnRow["field_subtype"].ToString()).DbType,
                Int32.Parse(columnRow["field_length"].ToString()))
            );
        }

        public IEnumerable<Procedure> GetStoredProcedures()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Parameter> GetParameters(Procedure storedProcedure)
        {
            throw new NotImplementedException();
        }

        public Key GetPrimaryKey(Table table)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ForeignKey> GetForeignKeys(Table table)
        {
            throw new NotImplementedException();
        }

        public string QuoteObjectName(string unquotedName)
        {
            if (String.IsNullOrEmpty(unquotedName)) throw new ArgumentNullException("unquotedName");
            return unquotedName.StartsWith("\"") ? unquotedName : String.Format("\"{0}\"", unquotedName);
        }

        public string NameParameter(string baseName)
        {
            throw new NotImplementedException();
        }

        public string GetDefaultSchema()
        {
            // Firebird does not support schemas
            return null;
        }

        private DataTable SelectToDataTable(string sql)
        {
            var dataTable = new DataTable();
            using (var conn = ConnectionProvider.CreateConnection() as FbConnection)
            {
                using (var adapter = new FbDataAdapter(sql, conn))
                {
                    adapter.Fill(dataTable);
                }
            }

            return dataTable;
        }
    }
}