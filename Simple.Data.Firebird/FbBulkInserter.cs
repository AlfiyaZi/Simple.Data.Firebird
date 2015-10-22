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

            var returnsColumnsSql = tableColumns.Select(c => c.ToSql()).ToList();
            var queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, returnsColumnsSql);

            foreach (var data in dataList)
            { //add list, add data, clean on execute procedure (or even better, pass to create and execute insert command) and call onError for all if exception occurs
                var insertData = data.Where(p => table.HasColumn(p.Key)).Select(kv => new InsertColumn
                {
                    Name = kv.Key,
                    Value = kv.Value,
                    Column = (FbColumn) table.FindColumn(kv.Key)
                }).ToArray();

                string insertSql = GetInsertSql(tableName, tableColumns, insertData, resultRequired);

                if (CanInsertInExecuteBlock(insertData, insertSql, queryBuilder))
                {

                    if (queryBuilder.CanAddQuery(insertSql)) queryBuilder.AddQuery(insertSql);
                    else
                    {
                        var subResult = CreateAndExecuteInsertCommand(transaction, queryBuilder.GetSql(), resultRequired);
                        if (resultRequired) result.AddRange(subResult);
                        queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, returnsColumnsSql);
                        queryBuilder.AddQuery(insertSql);
                    }
                }
                else
                {
                    var subResult = _inserter.Value.Insert(adapter, tableName, data, transaction, resultRequired);
                    if (resultRequired) result.Add(subResult);
                }
            }

            if (queryBuilder.QueryCount > 0)
            {
                var subResult = CreateAndExecuteInsertCommand(transaction, queryBuilder.GetSql(), resultRequired);
                if (resultRequired) result.AddRange(subResult);
            }

            return result;
        }

        
        private bool CanInsertInExecuteBlock(InsertColumn[] data, string insertSql, FbBulkInsertQueryBuilder queryBuilder)
        {
            return queryBuilder.SizeOf(insertSql) <= queryBuilder.MaximumQuerySize &&
                   data.All(ic => ic.Value == null || !ic.Value.GetType().IsArray);
        }

        private IEnumerable<IDictionary<string, object>> CreateAndExecuteInsertCommand(IDbTransaction transaction, string executeBlockSql, bool resultRequired)
        {
            using (var command = transaction.Connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = executeBlockSql;

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

        private string GetInsertSql(string tableName, FbColumn[] tableColumns, InsertColumn[] insertData, bool resultRequired)
        {
            string columnsSql = String.Join(",", insertData.Select(s => s.Column.QuotedName));
            string valuesSql = String.Join(",", insertData.Select(c => c.ValueToSql()));

            if (resultRequired)
            {
                string returnsColumnSql = String.Join(",", tableColumns.Select(c => c.QuotedName));
                string returnsVariablesSql = String.Join(",", tableColumns.Select(c => ":" + c.QuotedName));
                return string.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING {3} into {4};suspend;",
                tableName, columnsSql, valuesSql, returnsColumnSql, returnsVariablesSql);
            }
            else return string.Format("INSERT INTO {0} ({1}) VALUES({2});", tableName, columnsSql, valuesSql);
        }
    }
}
