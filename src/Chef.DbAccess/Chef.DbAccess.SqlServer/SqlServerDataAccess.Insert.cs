using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Transactions;
using Chef.DbAccess.SqlServer.Extensions;
using Dapper;

namespace Chef.DbAccess.SqlServer
{
    public partial class SqlServerDataAccess<T>
    {
        public virtual Task<int> InsertAsync(T value, Expression<Func<T, bool>> nonexistence = null)
        {
            if (nonexistence != null)
            {
                var parameters = value.ExtractRequired();

                var sql = this.GenerateInsertStatement(nonexistence: nonexistence, parameters: parameters);

                return this.ExecuteCommandAsync(sql, parameters);
            }
            else
            {
                var sql = this.GenerateInsertStatement();

                return this.ExecuteCommandAsync(sql, value);
            }
        }

        public virtual Task<T> InsertAsync(T value, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null)
        {
            if (nonexistence != null)
            {
                var parameters = value.ExtractRequired();

                var sql = this.GenerateInsertStatement(output, nonexistence, parameters);

                return this.ExecuteQueryOneAsync<T>(sql, parameters);
            }
            else
            {
                var sql = this.GenerateInsertStatement(output);

                return this.ExecuteQueryOneAsync<T>(sql, value);
            }
        }

        public virtual Task<int> InsertAsync(Expression<Func<T>> setter, Expression<Func<T, bool>> nonexistence = null)
        {
            var (sql, parameters) = nonexistence != null
                                        ? this.GenerateInsertStatement(setter, true, nonexistence: nonexistence)
                                        : this.GenerateInsertStatement(setter, true);

            return this.ExecuteCommandAsync(sql, parameters);
        }

        public virtual Task<T> InsertAsync(Expression<Func<T>> setter, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null)
        {
            var (sql, parameters) = nonexistence != null
                                        ? this.GenerateInsertStatement(setter, true, output, nonexistence)
                                        : this.GenerateInsertStatement(setter, true, output);

            return this.ExecuteQueryOneAsync<T>(sql, parameters);
        }

        public virtual Task<int> InsertAsync(IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null)
        {
            var sql = nonexistence != null ? this.GenerateInsertStatement(nonexistence: nonexistence) : this.GenerateInsertStatement();

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual Task<List<T>> InsertAsync(IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null)
        {
            var statements = nonexistence != null
                                 ? this.GenerateInsertStatement(output, nonexistence).Split(';')
                                 : this.GenerateInsertStatement(output).Split(';');

            return Transaction.Current != null
                       ? this.ExecuteQueryAsync<T>(statements[1], values, statements[2], null, preSql: statements[0])
                       : this.ExecuteTransactionalQueryAsync<T>(statements[1], values, statements[2], null, preSql: statements[0]);
        }

        public virtual Task<int> InsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null)
        {
            var (sql, _) = nonexistence != null
                               ? this.GenerateInsertStatement(setterTemplate, false, nonexistence: nonexistence)
                               : this.GenerateInsertStatement(setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual Task<List<T>> InsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null)
        {
            var (sql, _) = nonexistence != null
                               ? this.GenerateInsertStatement(setterTemplate, false, output, nonexistence)
                               : this.GenerateInsertStatement(setterTemplate, false, output);

            var statements = sql.Split(';');

            return Transaction.Current != null
                       ? this.ExecuteQueryAsync<T>(statements[1], values, statements[2], null, preSql: statements[0])
                       : this.ExecuteTransactionalQueryAsync<T>(statements[1], values, statements[2], null, preSql: statements[0]);
        }

        public virtual Task<int> BulkInsertAsync(IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null)
        {
            if (nonexistence != null)
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(values, nonexistence: nonexistence);

                return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
            else
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(values);

                return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
        }

        public virtual Task<List<T>> BulkInsertAsync(IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null)
        {
            if (nonexistence != null)
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(values, output, nonexistence);

                return this.ExecuteQueryAsync<T>(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
            else
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(values, output);

                return this.ExecuteQueryAsync<T>(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
        }

        public virtual Task<int> BulkInsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null)
        {
            if (nonexistence != null)
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(setterTemplate, values, nonexistence: nonexistence);

                return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
            else
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(setterTemplate, values);

                return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
        }

        public virtual Task<List<T>> BulkInsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null)
        {
            if (nonexistence != null)
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(setterTemplate, values, output, nonexistence);

                return this.ExecuteQueryAsync<T>(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
            else
            {
                var (preSql, sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(setterTemplate, values, output);

                return this.ExecuteQueryAsync<T>(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) }, preSql: preSql);
            }
        }
    }
}