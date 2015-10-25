using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.Linq;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado;
using Simple.Data.Extensions;

namespace Simple.Data.Firebird.BulkInsert
{
    [Export(typeof(IBulkInserter))]
    public class FbBulkInserter : IBulkInserter
    {
        public IEnumerable<IDictionary<string, object>> Insert(AdoAdapter adapter, string tableName, IEnumerable<IDictionary<string, object>> dataList, IDbTransaction transaction, Func<IDictionary<string, object>, Exception, bool> onError,
            bool resultRequired)
        {
            //ToDo: support onError collection
            List<IDictionary<string, object>> result = new List<IDictionary<string, object>>();
          
            if (transaction == null)
            {
                adapter.InTransaction(currentTransaction =>
                {
                    result = (List<IDictionary<string, object>>) Insert(adapter, tableName, dataList, currentTransaction, onError, resultRequired);
                });
                return result;
            }
            
            var tableColumns = adapter.GetSchema().FindTable(tableName).Columns.Select(c => (FbColumn)c).ToArray();
            var nameToFbColumns = tableColumns.ToDictionary(c => c.HomogenizedName, c => c);

            var insertContext = CreateInsertSqlContext(tableName, tableColumns);

            var queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, insertContext.ReturnsExecuteBlockSql);
            var insertSqlProvider = new FbBulkInsertSqlProvider();
            var currentColumns = new List<InsertColumn>();

            foreach (var data in dataList)
            {
                var insertData = data.Where(p => nameToFbColumns.ContainsKey(p.Key.Homogenize())).Select(kv => new InsertColumn
                {
                    Value = kv.Value,
                    Column =  nameToFbColumns[kv.Key.Homogenize()]
                }).ToArray();

                ExecuteBlockInsertSql insertSql = insertSqlProvider.GetInsertSql(insertContext, insertData, resultRequired);

                if (insertContext.SkipCommandParameters && !CanInsertInExecuteBlock(insertSql.InsertSql, queryBuilder))
                {
                    insertSql = insertSqlProvider.GetInsertSql(insertContext, insertData, resultRequired, skipCommandParameters: false); 
                }

                if (queryBuilder.CanAddQuery(insertSql))
                {
                    queryBuilder.AddQuery(insertSql);
                    if (!insertContext.SkipCommandParameters) currentColumns.AddRange(insertData);
                }
                else
                {
                    var subResult = CreateAndExecuteInsertCommand(transaction, currentColumns, queryBuilder.GetSql(), resultRequired);
                    if (resultRequired) result.AddRange(subResult);
                    currentColumns.Clear();

                    queryBuilder = new FbBulkInsertQueryBuilder(resultRequired, insertContext.ReturnsExecuteBlockSql);
                    queryBuilder.AddQuery(insertSql);
                    if (!insertContext.SkipCommandParameters) currentColumns.AddRange(insertData);
                }
            }

            if (queryBuilder.QueryCount > 0)
            {
                var subResult = CreateAndExecuteInsertCommand(transaction, currentColumns, queryBuilder.GetSql(), resultRequired);
                if (resultRequired) result.AddRange(subResult);
            }

            return result;
        }

        private FbInsertSqlContext CreateInsertSqlContext(string tableName, FbColumn[] tableColumns)
        {
            return new FbInsertSqlContext
            {
                TableName = tableName,
                ReturnsExecuteBlockSql = String.Join(",", tableColumns.Select(c => c.NameTypeSql)),
                ReturnsColumnSql = String.Join(",", tableColumns.Select(c => c.QuotedName)),
                ReturnsVariablesSql = String.Join(",", tableColumns.Select(c => ":" + c.QuotedName)),
                MaxParameterId = tableColumns.Length * FbBulkInsertQueryBuilder.MaximumExecuteBlockQueries,
                LastParameterId = -1,
                SkipCommandParameters = BulkInserterConfiguration.UseFasterUnsafeBulkInsertMethod
            };
        }

        private bool CanInsertInExecuteBlock(string insertSql, FbBulkInsertQueryBuilder queryBuilder)
        {
            return queryBuilder.SizeOf(insertSql) <= queryBuilder.MaximumQuerySize;
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
