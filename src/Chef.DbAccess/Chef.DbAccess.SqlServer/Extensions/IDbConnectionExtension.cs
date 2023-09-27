using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class IDbConnectionExtension
    {
        public static async Task<T> TryQuerySingleOrDefaultAsync<T>(
            this IDbConnection cnn,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            T result;

            try
            {
                result = await cnn.QuerySingleOrDefaultAsync<T>(sql, param, transaction, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QuerySingleOrDefaultAsync<T>(sql, param, transaction, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<IEnumerable<TReturn>> TryQueryAsync<TFirst, TSecond, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string splitOn = "Id",
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            IEnumerable<TReturn> result;

            try
            {
                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<IEnumerable<TReturn>> TryQueryAsync<TFirst, TSecond, TThird, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string splitOn = "Id",
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            IEnumerable<TReturn> result;

            try
            {
                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<IEnumerable<TReturn>> TryQueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string splitOn = "Id",
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            IEnumerable<TReturn> result;

            try
            {
                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<IEnumerable<TReturn>> TryQueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string splitOn = "Id",
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            IEnumerable<TReturn> result;

            try
            {
                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<IEnumerable<TReturn>> TryQueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string splitOn = "Id",
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            IEnumerable<TReturn> result;

            try
            {
                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<IEnumerable<TReturn>> TryQueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(
            this IDbConnection cnn,
            string sql,
            Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map,
            object param = null,
            IDbTransaction transaction = null,
            bool buffered = true,
            string splitOn = "Id",
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            IEnumerable<TReturn> result;

            try
            {
                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QueryAsync(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<IEnumerable<T>> TryQueryAsync<T>(
            this IDbConnection cnn,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            IEnumerable<T> result;

            try
            {
                result = await cnn.QueryAsync<T>(sql, param, transaction, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.QueryAsync<T>(sql, param, transaction, commandTimeout, commandType);
            }

            return result;
        }

        public static async Task<int> TryExecuteAsync(
            this IDbConnection cnn,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            int result;

            try
            {
                result = await cnn.ExecuteAsync(sql, param, transaction, commandTimeout, commandType);
            }
            catch (SqlException sqlEx) when (sqlEx.Number is 137 or 102)
            {
                SqlMapper.PurgeQueryCache();

                result = await cnn.ExecuteAsync(sql, param, transaction, commandTimeout, commandType);
            }

            return result;
        }
    }
}