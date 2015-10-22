using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Simple.Data.Firebird
{
    class FbBulkInsertQueryBuilder
    {
        public int QueryCount { get { return _queries.Count; } }
        public int MaximumQuerySize { get; private set; }

        private const int MaximumExecuteBlockQueries = 255;
        private const int MaximumExecuteBlockSize = 65535;

        private readonly string _executeBlockTemplate = "execute block as begin {0} end";
        private readonly string _executeBlockWithReturnsTemplate = "execute block returns ({1}) as begin {0} end";

        private readonly IEnumerable<string> _returnParams;
        private readonly List<string> _queries = new List<string>();

        private readonly string _currentTemplate;
        private int _currentSize;

        internal FbBulkInsertQueryBuilder(bool shouldReturnResluts, IEnumerable<string> returnParams = null)
        {
            _returnParams = returnParams;

            _currentTemplate = shouldReturnResluts ? _executeBlockWithReturnsTemplate : _executeBlockTemplate;
            _currentSize = SizeOf(GetSql());

            MaximumQuerySize = MaximumExecuteBlockSize - _currentSize;

            //ToDo: based on Firebird version (<=2.5.x or >= 3.0.x) pick proper size for max count / length of execute block
        }

        internal bool CanAddQuery(string query)
        {
            return _currentSize + SizeOf(query) <= MaximumExecuteBlockSize && _queries.Count < MaximumExecuteBlockQueries;
        }

        internal void AddQuery(string query)
        {
            if (!CanAddQuery(query)) throw new InvalidOperationException("Execute block size or query count exceeded maximum value.");
            _queries.Add(query);
            _currentSize += SizeOf(query);
        }

        internal string GetSql()
        {
            return String.Format(
                _currentTemplate, 
                String.Concat(_queries), 
                _returnParams != null ? String.Join(",", _returnParams) : ""
            );
        }

        internal int SizeOf(string str)
        {
            return Encoding.UTF8.GetByteCount(str);
        }
    }
}
