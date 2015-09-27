using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado;
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
            return SelectToDataTable(Resources.ProceduresQuery)
                .AsEnumerable()
                .Select(proc => new Procedure(proc["procedure_name"].ToString(), proc["procedure_name"].ToString(), null));
        }

        public IEnumerable<Parameter> GetParameters(Procedure storedProcedure)
        {
            var result = SelectToDataTable(String.Format(Resources.ParametersQuery, storedProcedure.Name))
                .AsEnumerable()
                .Select(param => new FbProcedureParameter(param["parameter_name"].ToString(),
                                                TypeMap.GetTypeEntry(param["field_type"].ToString(), param["field_subtype"].ToString()).ClrType,
                                                GetParameterDirection(param["parameter_direction"]),
                                                (short)param["parameter_number"], (int)param["is_optional"] == 1));
            return result;
        }

        private ParameterDirection GetParameterDirection(object direction)
        {
            return (short)direction == 0 ? ParameterDirection.Input : ParameterDirection.Output;
        }

        public Key GetPrimaryKey(Table table)
        {
            return new Key(SelectToDataTable(String.Format(Resources.PrimaryKeyQuery, table.ActualName))
                .AsEnumerable()
                .Select(columnRow => columnRow["field_name"].ToString()));
        }

        public IEnumerable<ForeignKey> GetForeignKeys(Table table)
        {
            var result = SelectToDataTable(String.Format(Resources.ForeignKeyQuery, table.ActualName))
                .AsEnumerable()
                .GroupBy(columnRow =>
                    new
                    {
                        Constraint = columnRow["constraint_name"].ToString(),
                        ReferenceTable = columnRow["reference_table"].ToString()
                    })
                .Select(group => new ForeignKey(new ObjectName(table.Schema, table.ActualName),
                    group.Select(columnRow => columnRow["field_name"].ToString()).Distinct(),
                    new ObjectName(table.Schema, group.Key.ReferenceTable),
                    group.Select(columnRow => columnRow["reference_field"].ToString()).Distinct()));

            return result;
        }

        public string QuoteObjectName(string unquotedName)
        {
            if (String.IsNullOrEmpty(unquotedName)) throw new ArgumentNullException("unquotedName");
            return unquotedName.StartsWith("\"") ? unquotedName : String.Format("\"{0}\"", unquotedName);
        }

        public string NameParameter(string baseName)
        {
            if (String.IsNullOrEmpty(baseName)) throw new ArgumentNullException("baseName");
            return baseName.StartsWith("@") ? baseName : String.Concat("@", baseName);
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