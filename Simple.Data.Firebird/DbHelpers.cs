using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Simple.Data.Ado;

namespace Simple.Data.Firebird
{
    internal static class DbHelpers
    {
        public static void InTransaction(this AdoAdapter adapter, Action<IDbTransaction> action)
        {
            using (var connection = adapter.ConnectionProvider.CreateConnection())
            {
                connection.Open();
                using (var currentTransaction = connection.BeginTransaction())
                {
                    action(currentTransaction);
                    try
                    {
                        currentTransaction.Commit();
                    }
                    catch (Exception)
                    {
                        currentTransaction.Rollback();
                        throw;
                    }
                }
            }
        }
    }
}
