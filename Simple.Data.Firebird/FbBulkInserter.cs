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
using Simple.Data.Extensions;

namespace Simple.Data.Firebird
{
    [Export(typeof(IBulkInserter))]
    public class FbBulkInserter : IBulkInserter
    {
        public IEnumerable<IDictionary<string, object>> Insert(AdoAdapter adapter, string tableName, IEnumerable<IDictionary<string, object>> dataList, IDbTransaction transaction, Func<IDictionary<string, object>, Exception, bool> onError,
            bool resultRequired)
        {
            //ToDo: support onError collection
            List<IDictionary<string, object>> result = new List<IDictionary<string, object>>();
            bool useFasterUnsafeMethod = BulkInserterConfiguration.UseFasterUnsafeBulkInsertMethod;

            if (transaction == null)
            {
                adapter.InTransaction(currentTransaction =>
                {
                    result = (List<IDictionary<string, object>>) Insert(adapter, tableName, dataList, currentTransaction, onError, resultRequired);
                });
                return result;
            }
            
            var table = adapter.GetSchema().FindTable(tableName);
            var tableColumns = table.Columns.Select(c => (FbColumn)c).ToArray();
            var returnTableColumns = tableColumns.Select(c => c.NameTypeSql).ToList();

            var availableColumnNames = new HashSet<string>(tableColumns.Select(tc => tc.HomogenizedName));

            var insertContext = new InsertSqlContext
            {
                TableName = tableName,
                ReturnsColumnSql = String.Join(",", tableColumns.Select(c => c.QuotedName)),
                ReturnsVariablesSql = String.Join(",", tableColumns.Select(c => ":" + c.QuotedName))
            };

            var queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, returnTableColumns);
            var currentColumns = new List<InsertColumn>();

            int currentId = 0;
            int maxId = FbBulkInsertQueryBuilder.MaximumExecuteBlockQueries*tableColumns.Length;

            foreach (var data in dataList)
            { //add list, add data, clean on execute procedure (or even better, pass to create and execute insert command) and call onError for all if exception occurs
                var insertData = data.Where(p => availableColumnNames.Contains(p.Key.Homogenize())).Select(kv => new InsertColumn
                {
                    Value = kv.Value,
                    Column = (FbColumn) table.FindColumn(kv.Key),
                    ParameterName = "p"+GetCurrentParameterId(ref currentId,maxId)
                }).ToArray();

                bool skipCommandParameters = false;
                ExecuteBlockInsertSql? insertSql = null;

                if (useFasterUnsafeMethod)
                {
                    var unsafeInsertSql = GetUnsafeInsertSql(insertContext, insertData, resultRequired);
                    if (CanInsertInExecuteBlock(insertData, unsafeInsertSql, queryBuilder))
                    {
                        insertSql = new ExecuteBlockInsertSql("", unsafeInsertSql, 0);
                        skipCommandParameters = true;
                    }
                }

                if (insertSql == null)
                {
                    insertSql = GetInsertSql(insertContext, insertData, resultRequired);
                }

                if (queryBuilder.CanAddQuery(insertSql.Value))
                {
                    queryBuilder.AddQuery(insertSql.Value);
                    if (!skipCommandParameters) currentColumns.AddRange(insertData);
                }
                else
                {
                    var subResult = CreateAndExecuteInsertCommand(transaction, currentColumns, queryBuilder.GetSql(), resultRequired);
                    if (resultRequired) result.AddRange(subResult);
                    currentColumns.Clear();

                    queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, returnTableColumns);
                    queryBuilder.AddQuery(insertSql.Value);
                    if (!skipCommandParameters) currentColumns.AddRange(insertData);
                }
            }

            if (queryBuilder.QueryCount > 0)
            {
                var subResult = CreateAndExecuteInsertCommand(transaction, currentColumns, queryBuilder.GetSql(), resultRequired);
                if (resultRequired) result.AddRange(subResult);
            }

            return result;
        }

        private static int GetCurrentParameterId(ref int currentId, int maxId)
        {
            int result = currentId++;
            if (currentId >= maxId) currentId = 0;
            return result;
        }

        private bool CanInsertInExecuteBlock(InsertColumn[] data, string insertSql, FbBulkInsertQueryBuilder queryBuilder)
        {
            return queryBuilder.SizeOf(insertSql) <= queryBuilder.MaximumQuerySize &&
                   data.All(ic => ic.Value == null || !ic.Value.GetType().IsArray);
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

        private ExecuteBlockInsertSql GetInsertSql(InsertSqlContext context, InsertColumn[] insertData, bool resultRequired)
        {
            string columnsSql = String.Join(",", insertData.Select(s => s.Column.QuotedName));
            string valuesSql = String.Join(",", insertData.Select(c => ":" + c.ParameterName));
            string parametersSql = String.Join(",", insertData.Select(c => String.Format("{0} {1}=@{0}", c.ParameterName, c.Column.TypeSql)));
            int parametersSize = insertData.Sum(id => id.Column.Size);
            string insertSql;

            if (resultRequired)
            {
                insertSql = String.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING {3} into {4};suspend;",
                    context.TableName, columnsSql, valuesSql, context.ReturnsColumnSql, context.ReturnsVariablesSql);                
            }
            else
            {
                insertSql = String.Format("INSERT INTO {0} ({1}) VALUES({2});", context.TableName, columnsSql, valuesSql);
            }
            return new ExecuteBlockInsertSql(parametersSql, insertSql, parametersSize);
        }

        private string GetUnsafeInsertSql(InsertSqlContext context, InsertColumn[] insertData, bool resultRequired)
        {
            string columnsSql = String.Join(",", insertData.Select(s => s.Column.QuotedName));
            string valuesSql = String.Join(",", insertData.Select(c => c.ValueToSql()));

            if (resultRequired)
            {
                return string.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING {3} into {4};suspend;",
                    context.TableName, columnsSql, valuesSql, context.ReturnsColumnSql, context.ReturnsVariablesSql);
            }
            else return string.Format("INSERT INTO {0} ({1}) VALUES({2});", context.TableName, columnsSql, valuesSql);
        }

        private class InsertSqlContext
        {
            public string TableName { get; set; }
            public string ReturnsColumnSql { get; set; }
            public string ReturnsVariablesSql { get; set; }
        }
    }

    public static class BulkInserterConfiguration
    {
        /// <summary>
        /// Configures Firebird provider to place column values for insert directly in sql string instead of using command parameters.
        /// Due to Firebird limitations this often allows to send more insert commands at once, especially if we're inserting short string values into large database columns. This speeds up insert process.
        /// Single quote is escaped for all objects (for which ToString method would be called) including strings, so this method is most likely safe, but it's still more dangerous than using command parameters.
        /// This method is not supported for arrays and strings that are too long to place directly in execute block.
        /// </summary>
        public static bool UseFasterUnsafeBulkInsertMethod { get; set; }
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
