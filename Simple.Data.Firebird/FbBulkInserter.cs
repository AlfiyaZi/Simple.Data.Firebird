using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.Globalization;
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
        private Lazy<FbInserter> _inserter = new Lazy<FbInserter>(); 

        public IEnumerable<IDictionary<string, object>> Insert(AdoAdapter adapter, string tableName, IEnumerable<IDictionary<string, object>> dataList, IDbTransaction transaction, Func<IDictionary<string, object>, Exception, bool> onError,
            bool resultRequired)
        {
            //ToDo: support onError collection
            List<IDictionary<string, object>> result = null;

            if (transaction == null)
            {
                adapter.InTransaction(currentTransaction =>
                {
                    result = (List<IDictionary<string, object>>) Insert(adapter, tableName, dataList, currentTransaction, onError, resultRequired);
                });
                return result;
            }

            if (resultRequired) result = new List<IDictionary<string, object>>();
            
            var table = adapter.GetSchema().FindTable(tableName);
            var tableColumns = table.Columns.Select(c => (FbColumn)c).ToArray();

            var returnsColumnsSql = tableColumns.Select(c => c.NameTypeSql).ToList();
            var queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, returnsColumnsSql);
            var currentColumns = new List<InsertColumn>();

            int currentId = 0;

            foreach (var data in dataList)
            { //add list, add data, clean on execute procedure (or even better, pass to create and execute insert command) and call onError for all if exception occurs
                var insertData = data.Where(p => table.HasColumn(p.Key)).Select(kv => new InsertColumn
                {
                    Name = kv.Key,
                    Value = kv.Value,
                    Column = (FbColumn) table.FindColumn(kv.Key),
                    ParameterName = "p"+(currentId++)
                }).ToArray();

                var insertSql = GetInsertSql(tableName, tableColumns, insertData, resultRequired);

                if (queryBuilder.CanAddQuery(insertSql))
                {
                    queryBuilder.AddQuery(insertSql);
                    currentColumns.AddRange(insertData);
                }
                else
                {
                    var subResult = CreateAndExecuteInsertCommand(transaction, currentColumns, queryBuilder.GetSql(), resultRequired);
                    if (resultRequired) result.AddRange(subResult);
                    currentColumns.Clear();

                    queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, returnsColumnsSql);
                    queryBuilder.AddQuery(insertSql);
                    currentColumns.AddRange(insertData);
                }
            }

            if (queryBuilder.QueryCount > 0)
            {
                var subResult = CreateAndExecuteInsertCommand(transaction, currentColumns, queryBuilder.GetSql(), resultRequired);
                if (resultRequired) result.AddRange(subResult);
            }

            return result;
        }

        private IEnumerable<IDictionary<string, object>> CreateAndExecuteInsertCommand(IDbTransaction transaction, List<InsertColumn> currentColumns, string executeBlockSql, bool resultRequired)
        {
            using (var command = transaction.Connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = executeBlockSql;

                foreach (var insertColumn in currentColumns)
                {
                    command.Parameters.Add(new FbParameter
                    {
                        ParameterName = "@"+insertColumn.ParameterName,
                        Value = insertColumn.Value,
                        Direction = ParameterDirection.Input
                    });
                }

                return ExecuteInsertCommand(command, resultRequired);
            }
        }

        private IEnumerable<IDictionary<string, object>> ExecuteInsertCommand(IDbCommand command, bool resultRequired)
        {
            if (resultRequired)
            {
                using (var rdr = command.ExecuteReader())
                {
                    return rdr.ToDictionaries();
                }
            }
            else
            {
                command.ExecuteNonQuery();
                return null;
            }
        }

        private ExecuteBlockInsertSql GetInsertSql(string tableName, FbColumn[] tableColumns, InsertColumn[] insertData, bool resultRequired)
        {
            string columnsSql = String.Join(",", insertData.Select(s => s.Column.QuotedName));
            string valuesSql = String.Join(",", insertData.Select(c => ":" + c.ParameterName));
            string parametersSql = String.Join(",", insertData.Select(c => String.Format("{0} {1}=@{0}", c.ParameterName, c.Column.TypeSql)));
            int parametersSize = insertData.Sum(id => id.Column.Size);
            string insertSql;

            if (resultRequired)
            {
                string returnsColumnSql = String.Join(",", tableColumns.Select(c => c.QuotedName));
                string returnsVariablesSql = String.Join(",", tableColumns.Select(c => ":" + c.QuotedName));

                insertSql = String.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING {3} into {4};suspend;",
                    tableName, columnsSql, valuesSql, returnsColumnSql, returnsVariablesSql);                
            }
            else
            {
                insertSql = String.Format("INSERT INTO {0} ({1}) VALUES({2});", tableName, columnsSql, valuesSql);
            }
            return new ExecuteBlockInsertSql(parametersSql, insertSql, parametersSize);
        }
    }

    internal struct ExecuteBlockInsertSql
    {
        internal readonly string ParametersSql;
        internal readonly string InsertSql;
        internal readonly int ParametersSize;

        public ExecuteBlockInsertSql(string parametersSql, string insertSql, int parametersSize)
        {
            ParametersSql = parametersSql;
            InsertSql = insertSql;
            ParametersSize = parametersSize;
        }
    }
}
