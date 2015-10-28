using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Simple.Data.Firebird.BulkInsert
{
    static class StringHelpers
    {
        internal static int GetSize(this string str)
        {
            return Encoding.UTF8.GetByteCount(str);
        }
    }
}
