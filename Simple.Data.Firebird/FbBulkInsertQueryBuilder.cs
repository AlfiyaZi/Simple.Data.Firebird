using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Simple.Data.Firebird
{
    class FbBulkInsertQueryBuilder
    {
        internal int QueryCount { get { return _insertSqls.Count; } }

        public const int MaximumExecuteBlockQueries = 255;
        private const int MaximumExecuteBlockSize = 65535;
        private const int MaximumExecuteBlockInputParametersSize = 65535;

        private readonly string _executeBlockTemplate = "execute block ({0}) as begin {1} end";
        private readonly string _executeBlockWithReturnsTemplate = "execute block ({0}) returns ({2}) as begin {1} end";

        private readonly IEnumerable<string> _returnParams;
        private readonly List<string> _insertSqls = new List<string>();
        private readonly List<string> _parametersSqls = new List<string>();

        private readonly string _currentTemplate;
        private int _currentBodySize;
        private int _currentInputParametersSize;

        internal FbBulkInsertQueryBuilder(bool shouldReturnResluts, IEnumerable<string> returnParams = null)
        {
            _returnParams = returnParams;

            _currentTemplate = shouldReturnResluts ? _executeBlockWithReturnsTemplate : _executeBlockTemplate;
            _currentBodySize = SizeOf(GetSql());

            //ToDo: based on Firebird version (<=2.5.x or >= 3.0.x) pick proper size for max count / length of execute block
        }

        internal bool CanAddQuery(ExecuteBlockInsertSql query)
        {
            return _currentBodySize + SizeOf(query.InsertSql) + SizeOf(query.ParametersSql) <= MaximumExecuteBlockSize &&
                   _currentInputParametersSize + query.ParametersSize <= MaximumExecuteBlockInputParametersSize &&
                   _insertSqls.Count < MaximumExecuteBlockQueries;
        }

        internal void AddQuery(ExecuteBlockInsertSql query)
        {
            _insertSqls.Add(query.InsertSql);
            _parametersSqls.Add(query.ParametersSql);
            _currentBodySize += (SizeOf(query.ParametersSql) + SizeOf(query.InsertSql));
            _currentInputParametersSize += query.ParametersSize;
        }

        internal string GetSql()
        {
            return String.Format(
                _currentTemplate,
                String.Join(",", _parametersSqls),
                String.Concat(_insertSqls), 
                _returnParams != null ? String.Join(",", _returnParams) : ""
            );
        }

        internal int SizeOf(string str)
        {
            return Encoding.UTF8.GetByteCount(str);
        }
    }
}
