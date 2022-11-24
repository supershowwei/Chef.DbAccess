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
        public virtual Task<int> UpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateUpdateStatement(predicate, setter, true);

            return this.ExecuteCommandAsync(sql, parameters);
        }

        public virtual Task<int> UpdateAsync<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateUpdateStatement(secondJoin, predicate, setter, true);

            return this.ExecuteCommandAsync(sql, parameters);
        }

        public virtual Task<int> UpdateAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateUpdateStatement(predicateTemplate, setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual Task<int> BulkUpdateAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkUpdateStatement(predicateTemplate, setterTemplate, values);

            return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }
    }
}