using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using FirebirdSql.Data.FirebirdClient;
using Simple.Data.Ado;
using Simple.Data.Ado.Schema;
using ResultSet = System.Collections.Generic.IEnumerable<System.Collections.Generic.IDictionary<string, object>>;

namespace Simple.Data.Firebird
{
    public class FbProcedureExecutor : IProcedureExecutor
    {
        public AdoAdapter Adapter { get; set; }
        public ObjectName ProcedureName { get; set; }

        public FbProcedureExecutor(AdoAdapter adapter, ObjectName procedureName)
        {
            Adapter = adapter;
            ProcedureName = procedureName;
        }

        public IEnumerable<IEnumerable<IDictionary<string, object>>> Execute(IDictionary<string, object> suppliedParameters)
        {
            return Execute(suppliedParameters, null);
        }

        public IEnumerable<IEnumerable<IDictionary<string, object>>> Execute(IDictionary<string, object> suppliedParameters, IDbTransaction transaction)
        {
            var procedure = DatabaseSchema.Get(Adapter.ConnectionProvider, Adapter.ProviderHelper).FindProcedure(ProcedureName);
            if (procedure == null)
            {
                throw new UnresolvableObjectException(ProcedureName.ToString());
            }

            if (transaction != null) return ExecuteProcedure(procedure, suppliedParameters, transaction.Connection, transaction);

            using (var connection = Adapter.CreateConnection())
            {
                connection.Open();
                return ExecuteProcedure(procedure, suppliedParameters, connection, null);
            }
        }

        private IEnumerable<ResultSet> ExecuteProcedure(Procedure procedure, IDictionary<string, object> suppliedParameters, IDbConnection connection, IDbTransaction transaction)
        {
            bool isQueryable = procedure.Parameters.Any(param => param.Direction == ParameterDirection.Output);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = procedure.QualifiedName;
                command.CommandType = CommandType.StoredProcedure;
                if (transaction != null) command.Transaction = transaction;
                AddCommandParameters(procedure, command, suppliedParameters);

                try
                {
                    var result = Enumerable.Empty<ResultSet>();

                    command.WriteTrace();
                    if (isQueryable)
                    {   
                        using (var rdr = command.ExecuteReader())
                        {
                            result = rdr.ToMultipleDictionaries();
                        }
                    }
                    else
                    {
                        command.ExecuteNonQuery();
                    }

                    return result;
                }
                catch (DbException ex)
                {
                    throw new AdoAdapterException(ex.Message, command, ex);
                }
            }
        }

        private void AddCommandParameters(Procedure procedure, IDbCommand command, IDictionary<string, object> suppliedParameters)
        {
            bool optionalParameterNotFound = false;
            string skippedOptionalParemeterName = null;

            foreach (var parameter in procedure.Parameters
                .Where(param => param.Direction == ParameterDirection.Input)
                .Select(param => (FbProcedureParameter) param))
            {
                var name = parameter.Name;
                object value;

                if (!suppliedParameters.TryGetValue(name, out value))
                {
                    name = String.Concat("_", parameter.Position);
                    if (!suppliedParameters.TryGetValue(name, out value))
                    {
                        if (parameter.Optional)
                        {
                            optionalParameterNotFound = true;
                            skippedOptionalParemeterName = parameter.Name;
                            continue;
                        }
                        throw new SimpleDataException(
                            String.Format("Could not find a value for parameter on position {0} named {1}",
                                parameter.Position, parameter.Name));
                    }
                }

                // we can skip optional parameters, but not from the middle of parameter list. For example if parameters 5-8 are optional, we can skip them all,
                // provide parameters 5,6,7 but not 5,7,8
                if (optionalParameterNotFound)
                {
                    throw new SimpleDataException(
                        String.Format(
                            @"Optional procedure parameter named {0} was provided, but parameter named {1} that is declared before it wasn't.",
                            parameter.Name, skippedOptionalParemeterName));
                }

                command.Parameters.Add(new FbParameter
                {
                    ParameterName = name,
                    Value = value,
                    Direction = ParameterDirection.Input
                });
            }
        }
    }
}