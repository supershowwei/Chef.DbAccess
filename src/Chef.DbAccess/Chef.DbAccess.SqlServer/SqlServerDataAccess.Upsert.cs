using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Transactions;
using Dapper;

namespace Chef.DbAccess.SqlServer
{
    public partial class SqlServerDataAccess<T>
    {
        public virtual Task<int> UpsertAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateUpsertStatement(predicate, setter, true);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, parameters) : this.ExecuteTransactionalCommandAsync(sql, parameters);
        }

        public virtual Task<T> UpsertAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter, Expression<Func<T, object>> output)
        {
            var (sql, parameters) = this.GenerateUpsertStatement(predicate, setter, true, output);

            return Transaction.Current != null
                       ? this.ExecuteQueryOneAsync<T>(sql, parameters)
                       : this.ExecuteTransactionalQueryOneAsync<T>(sql, parameters);
        }

        public virtual Task<int> UpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateUpsertStatement(predicateTemplate, setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual Task<List<T>> UpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output)
        {
            var (sql, _) = this.GenerateUpsertStatement(predicateTemplate, setterTemplate, false, output);
            var statements = sql.Split(';');

            return Transaction.Current != null
                       ? this.ExecuteQueryAsync<T>(statements[1], values, statements[2], null, preSql: statements[0])
                       : this.ExecuteTransactionalQueryAsync<T>(statements[1], values, statements[2], null, preSql: statements[0]);
        }

        public virtual Task<int> BulkUpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkUpsertStatement(predicateTemplate, setterTemplate, values);

            return Transaction.Current != null
                       ? this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) })
                       : this.ExecuteTransactionalCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual Task<List<T>> BulkUpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkUpsertStatement(predicateTemplate, setterTemplate, values, output);

            return Transaction.Current != null
                       ? this.ExecuteQueryAsync<T>(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) })
                       : this.ExecuteTransactionalQueryAsync<T>(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }
    }
}