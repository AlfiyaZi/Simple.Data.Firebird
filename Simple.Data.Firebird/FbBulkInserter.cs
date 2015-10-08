using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.Linq;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado;
using Simple.Data.Ado.Schema;

namespace Simple.Data.Firebird
{
    [Export(typeof(IBulkInserter))]
    public class FbBulkInserter : IBulkInserter
    {
        private const int MaximumExecuteBlockQueries = 255;

        public IEnumerable<IDictionary<string, object>> Insert(AdoAdapter adapter, string tableName, IEnumerable<IDictionary<string, object>> dataList, IDbTransaction transaction, Func<IDictionary<string, object>, Exception, bool> onError,
            bool resultRequired)
        {
            //test only
            resultRequired = false;

            IEnumerable<IDictionary<string, object>> result = null;
            var table = adapter.GetSchema().FindTable(tableName);

            var insertGroups = dataList.Select((data, idx) => new { Data = data, Id = idx }).GroupBy(data => data.Id / MaximumExecuteBlockQueries);
            foreach (var insertGroup in insertGroups)
            {
                int parameterIndex = 0;
                var insertColumnsValues = insertGroup.Select(data =>
                {
                    var insertData = data.Data.Where(p => table.HasColumn(p.Key)).Select(kv => new InsertColumn
                    {
                        Name = kv.Key,
                        ParameterName = "@p" + parameterIndex++,
                        Value = kv.Value,
                        Column = table.FindColumn(kv.Key)
                    }).ToArray();

                    return new
                    {
                        InsertData = insertData,
                        InsertSqlLine = GetInsertSql(table, insertData, resultRequired)
                    };
                });

                if (transaction == null)
                {
                    adapter.InTransaction(currentTransaction =>
                    {
                        var insertColumnsValuesList = insertColumnsValues.ToList();
                        string executeBlockSql = String.Format(GetExecuteBlockTemplate(resultRequired),
                            String.Join(Environment.NewLine, insertColumnsValuesList.Select(ig => ig.InsertSqlLine)));

                        CreateAndExecuteInsertCommand(currentTransaction.Connection, table, executeBlockSql,
                            insertColumnsValuesList.SelectMany(cv => cv.InsertData).ToArray(), resultRequired,
                            currentTransaction);
                    });
                }
            }

            return result;
        }

        private IDictionary<string, object> CreateAndExecuteInsertCommand(IDbConnection connection, Table table, string executeBlockSql, InsertColumn[] insertColumns, bool resultRequired, IDbTransaction transaction = null)
        {
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = executeBlockSql;

                //foreach (var insertColumn in insertColumns)
                //{
                //    command.Parameters.Add(new FbParameter
                //    {
                //        ParameterName = insertColumn.ParameterName,
                //        Value = insertColumn.Value,
                //        Direction = resultRequired ? ParameterDirection.InputOutput : ParameterDirection.Input
                //    });
                //}

                return ExecuteInsertCommand(command, resultRequired);
            }
        }

        private IDictionary<string, object> ExecuteInsertCommand(IDbCommand command, bool resultRequired)
        {
            command.ExecuteNonQuery();

            if (resultRequired)
            {
                return null;
                    //columnParameters.ToDictionary(p => p.Key, p => p.Value.Value is DBNull ? null : p.Value.Value);
            }

            return null;
        }

        private string GetExecuteBlockTemplate(Table table, bool resultRequired)
        {
            if (resultRequired)
                return null;//String.Join(Environment.NewLine, String.Format("execute block returns ({0}) as begin", table.Columns.Select(c => c.QuotedName + " " + c.)), "{0}", "end");
            return String.Join(Environment.NewLine, "execute block as begin", "{0}", "end");
        }

        private string GetInsertSql(Table table, InsertColumn[] insertData, bool resultRequired)
        {
            string columnsSql = String.Join(",", insertData.Select(s => s.Column.QuotedName));
            string valuesSql = String.Join(",", insertData.Select(c => c.Value != null ? String.Format("'{0}'", c.Value) : "null"));

            if (resultRequired)
            {
                string returnsSql = String.Join(",", insertData.Select(c => ":" + c.Column.QuotedName));
                return string.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING {1} into {3};suspend;",
                table.QualifiedName, columnsSql, valuesSql, returnsSql);
            }
            else return string.Format("INSERT INTO {0} ({1}) VALUES({2});", table.QualifiedName, columnsSql, valuesSql);
        }
    }
}
