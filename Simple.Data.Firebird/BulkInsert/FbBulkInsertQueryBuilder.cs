using System;
using System.Collections.Generic;
using System.Text;

namespace Simple.Data.Firebird.BulkInsert
{
    class FbBulkInsertQueryBuilder
    {
        internal int QueryCount { get { return _insertSqls.Count; } }
        internal int MaximumQuerySize { get; private set; }

        internal const int MaximumExecuteBlockQueries = 255;
        private const int MaximumExecuteBlockSize = 65535;
        private const int MaximumExecuteBlockInputParametersSize = 65535;
        private const int SizeOfParametersSeparator = 1;

        private readonly string _executeBlockTemplate = "execute block {0}as begin {1} end";
        private readonly string _executeBlockWithReturnsTemplate = "execute block {0}returns ({2}) as begin {1} end";

        private readonly string _returnParamsSql;
        private readonly List<string> _insertSqls = new List<string>();
        private readonly List<string> _parametersSqls = new List<string>();

        private readonly string _currentTemplate;
        private int _currentBodySize;
        private int _currentInputParametersSize;

        internal FbBulkInsertQueryBuilder(bool shouldReturnResluts, string returnParamsSql)
        {
            _returnParamsSql = returnParamsSql;

            _currentTemplate = shouldReturnResluts ? _executeBlockWithReturnsTemplate : _executeBlockTemplate;
            _currentBodySize = GetSql().GetSize();

            MaximumQuerySize = MaximumExecuteBlockSize - _currentBodySize;

            //ToDo: based on Firebird version (<=2.5.x or >= 3.0.x) pick proper size for max count / length of execute block
        }

        internal bool CanAddQuery(ExecuteBlockInsertSql query)
        {
            return _currentBodySize + SizeOf(query) <= MaximumExecuteBlockSize &&
                   _currentInputParametersSize + query.ParametersSize <= MaximumExecuteBlockInputParametersSize &&
                   _insertSqls.Count < MaximumExecuteBlockQueries;
        }

        internal void AddQuery(ExecuteBlockInsertSql query)
        {
            _currentBodySize += SizeOf(query);
            _currentInputParametersSize += query.ParametersSize;
            _insertSqls.Add(query.InsertSql);
            if (!String.IsNullOrEmpty(query.ParametersSql)) _parametersSqls.Add(query.ParametersSql);
        }

        internal string GetSql()
        {
            return String.Format(
                _currentTemplate,
                _parametersSqls.Count > 0 ? String.Format("({0}) ", String.Join(",", _parametersSqls)) : "",
                String.Concat(_insertSqls), 
                _returnParamsSql != null ? String.Join(",", _returnParamsSql) : ""
            );
        }

        private int SizeOf(ExecuteBlockInsertSql query)
        {
            return query.InsertSql.GetSize() 
                + query.ParametersSql.GetSize() 
                + (!String.IsNullOrEmpty(query.ParametersSql) && _parametersSqls.Count > 0 ? SizeOfParametersSeparator : 0);
        }
    }
}
