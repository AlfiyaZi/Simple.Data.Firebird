using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Simple.Data.Ado;

namespace Simple.Data.Firebird
{
    [Export(typeof(IQueryPager))]
    public class FbQueryPager : IQueryPager
    {
        public IEnumerable<string> ApplyLimit(string sql, int take)
        {
            yield return String.Format("{0} ROWS {1}", sql, take);
        }

        public IEnumerable<string> ApplyPaging(string sql, string[] keys, int skip, int take)
        {
            long fromRow = skip + 1;
            long toRow = fromRow + take;    // using long, as Simple.Data passes Int32.MaxValue as take parameter with skip only option and we don't want to deal with int overflow
            yield return String.Format("{0} ROWS {1} TO {2}", sql, fromRow, toRow);
        }
    }
}
