using System;
using System.Collections.Generic;

namespace Simple.Data.Firebird.BulkInsert
{
    public class FbBulkInsertSqlProvider
    {
        const int ParameterSizeInBytes = 4;

        internal ExecuteBlockInsertSql GetInsertSql(FbInsertSqlContext insertContext, InsertColumn[] insertValues, bool resultRequired)
        {
            return GetInsertSql(insertContext, insertValues, resultRequired, insertContext.SkipCommandParameters);
        }

        internal ExecuteBlockInsertSql GetInsertSql(FbInsertSqlContext insertContext, InsertColumn[] insertValues, bool resultRequired, bool skipCommandParameters)
        {
            List<string> columnsList = new List<string>(insertValues.Length);
            List<string> valuesList = new List<string>(insertValues.Length);
            List<string> parametersList = new List<string>(insertValues.Length);
            int parametersSize = 0;

            for (int i = 0; i < insertValues.Length; i++)
            {
                var currentValue = insertValues[i];
                columnsList.Add(currentValue.Column.QuotedName);

                if (skipCommandParameters && CanSkipCommandParameterFor(currentValue))
                {
                    valuesList.Add(currentValue.ValueToSql());
                }
                else
                {
                    if (currentValue.ParameterName == null)
                        currentValue.ParameterName = GetNextParameterName(insertContext);

                    valuesList.Add(":" + currentValue.ParameterName);
                    parametersList.Add(String.Format("{0} {1}=@{0}", currentValue.ParameterName,
                        currentValue.ValueParameterToSql()));
                    parametersSize += currentValue.Column.Size + ParameterSizeInBytes;
                }
            }

            string insertSql;
            if (resultRequired)
            {
                insertSql = String.Format("INSERT INTO {0} ({1}) VALUES({2}) RETURNING {3} into {4};suspend;",
                    insertContext.TableName, String.Join(",", columnsList), String.Join(",", valuesList), insertContext.ReturnsColumnSql, insertContext.ReturnsVariablesSql);
            }
            else
            {
                insertSql = String.Format("INSERT INTO {0} ({1}) VALUES({2});", insertContext.TableName, String.Join(",", columnsList), String.Join(",", valuesList));
            }

            return new ExecuteBlockInsertSql(String.Join(",", parametersList), insertSql, parametersSize);
        }

        private string GetNextParameterName(FbInsertSqlContext insertContext)
        {
            int currentId = ++insertContext.LastParameterId;
            if (insertContext.LastParameterId >= insertContext.MaxParameterId) insertContext.LastParameterId = 0;
            return "p" + currentId;
        }

        private bool CanSkipCommandParameterFor(InsertColumn insertColumn)
        {
            return insertColumn.Value == null || !insertColumn.Value.GetType().IsArray;
        }
    }
}