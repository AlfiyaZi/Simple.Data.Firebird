using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado;
using Simple.Data.Ado.Schema;
using Simple.Data.Commands;
using Simple.Data.Extensions;

namespace Simple.Data.Firebird
{
    [Export(typeof(ICustomInserter))]
    public class FbCustomInserter : ICustomInserter
    {
        public IDictionary<string, object> Insert(AdoAdapter adapter, string tableName, IDictionary<string, object> data, IDbTransaction transaction = null,
            bool resultRequired = false)
        {
            var table = adapter.GetSchema().FindTable(tableName);

            var insertData = data.Where(p => table.HasColumn(p.Key)).Select((kv, idx) => new InsertColumn
            {
                Name = kv.Key,
                ParameterName = "@p" + idx,
                Value = kv.Value,
                Column = table.FindColumn(kv.Key)
            }).ToArray();

            if (transaction == null)
            {
                using (var connection = adapter.ConnectionProvider.CreateConnection())
                {
                    connection.Open();
                    return CreateCommand(connection, table, insertData, resultRequired);
                }
            }
            else
            {
                return CreateCommand(transaction.Connection, table, insertData, resultRequired, transaction);
            }
        }

        private IDictionary<string, object> CreateCommand(IDbConnection connection, Table table, InsertColumn[] insertColumns, bool resultRequired, IDbTransaction transaction = null)
        {
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = GetInsertSql(table, insertColumns, resultRequired);

                var columnParameters = new Dictionary<string,FbParameter>(insertColumns.Length);

                foreach (var insertColumn in insertColumns)
                {
                    columnParameters[insertColumn.Name] = new FbParameter
                    {
                        ParameterName = insertColumn.ParameterName,
                        Value = insertColumn.Value,
                        Direction = resultRequired ? ParameterDirection.InputOutput : ParameterDirection.Input
                    };

                    command.Parameters.Add(columnParameters[insertColumn.Name]);
                }

                return ExecuteInsertCommand(command, columnParameters, resultRequired);
            }
        }

        private IDictionary<string, object> ExecuteInsertCommand(IDbCommand command, Dictionary<string, FbParameter> columnParameters, bool resultRequired)
        {
            command.ExecuteNonQuery();

            if (resultRequired)
            {
                return columnParameters.ToDictionary(p => p.Key, p => p.Value.Value is DBNull ? null : p.Value.Value);
            }

            return null;
        }

        private string GetInsertSql(Table table, InsertColumn[] insertData, bool resultRequired)
        {
            string columnsSql = String.Join(",", insertData.Select(s => s.Column.QuotedName));
            string valuesSql = String.Join(",", insertData.Select(c => c.ParameterName));
            
            if (resultRequired) return string.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING {1};", table.QualifiedName, columnsSql, valuesSql);
            else return string.Format("INSERT INTO {0} ({1}) VALUES({2});", table.QualifiedName, columnsSql, valuesSql);
        }


        private class InsertColumn
        {
            public string Name { get; set; }
            public object Value { get; set; }
            public string ParameterName { get; set; }
            public Column Column { get; set; }
        }
    }
}
