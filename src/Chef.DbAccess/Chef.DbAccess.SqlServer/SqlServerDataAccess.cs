﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Transactions;
using Chef.DbAccess.SqlServer.Extensions;
using Dapper;

[assembly: InternalsVisibleTo("Chef.DbAccess.SqlServer.Tests")]

namespace Chef.DbAccess.SqlServer
{
    public abstract class SqlServerDataAccess
    {
        protected static readonly Regex ColumnValueRegex = new Regex(@"(\[[^\]]+\]) [^\s]+ ([_0-9a-zA-Z]+\.)?([@\{\[]=?[^,\s\}\)]+(_[\d]+)?\]?\}?)");
        protected static readonly Regex ColumnRegex = new Regex(@"\[[^\]]+\]");

        protected SqlServerDataAccess()
        {
        }
    }

    public class SqlServerDataAccess<T> : SqlServerDataAccess, IDataAccess<T>
    {
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> RequiredColumns = new ConcurrentDictionary<Type, PropertyInfo[]>();
        private static readonly ConcurrentDictionary<string, Delegate> Setters = new ConcurrentDictionary<string, Delegate>();
        private static readonly Regex ServerRegex = new Regex(@"(Server|Data Source)=[\s]*([^;]+)[\s]*;", RegexOptions.IgnoreCase);
        private static readonly Regex DatabaseRegex = new Regex(@"(Database|Initial Catalog)=[\s]*([^;]+)[\s]*;", RegexOptions.IgnoreCase);
        private readonly string connectionString;
        private readonly string tableName;
        private readonly string alias;

        public SqlServerDataAccess(string connectionString)
        {
            this.connectionString = connectionString;

            this.tableName = typeof(T).GetCustomAttribute<TableAttribute>()?.Name ?? typeof(T).Name;
            this.alias = GenerateAlias(typeof(T), 1);

            this.IsDirtyRead = true;
        }

        public bool IsDirtyRead { get; set; }

        public Action<string, IDictionary<string, object>> OutputSql { get; set; }

        public virtual T QueryOne(string sql, object param)
        {
            return this.ExecuteQueryOne<T>(sql, param);
        }

        public virtual Task<T> QueryOneAsync(string sql, object param)
        {
            return this.ExecuteQueryOneAsync<T>(sql, param);
        }

        public virtual T QueryOne(
            Expression<Func<T, bool>> predicate,
            IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null,
            Expression<Func<T, object>> selector = null,
            Expression<Func<T, object>> groupingColumns = null,
            Expression<Func<Grouping<T>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters) = this.GenerateQueryStatement(this.tableName, this.alias, predicate, orderings, selector, groupingColumns, groupingSelector, skipped, taken);

            return this.ExecuteQueryOne<T>(sql, parameters);
        }

        public virtual Task<T> QueryOneAsync(
            Expression<Func<T, bool>> predicate,
            IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null,
            Expression<Func<T, object>> selector = null,
            Expression<Func<T, object>> groupingColumns = null,
            Expression<Func<Grouping<T>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters) = this.GenerateQueryStatement(this.tableName, this.alias, predicate, orderings, selector, groupingColumns, groupingSelector, skipped, taken);

            return this.ExecuteQueryOneAsync<T>(sql, parameters);
        }

        public virtual T QueryOne<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, object>> selector = null,
            Expression<Func<T, TSecond, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQueryOne<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = db.Query<T, TSecond, T>(
                    sql,
                    (first, second) =>
                        {
                            secondSetter(first, second);

                            return first;
                        },
                    parameters,
                    splitOn: splitOn);

                return result.SingleOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, object>> selector = null,
            Expression<Func<T, TSecond, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, T>(
                                     sql,
                                     (first, second) =>
                                         {
                                             secondSetter(first, second);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.SingleOrDefault();
                }
            }
        }

        public virtual T QueryOne<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, object>> selector = null,
            Expression<Func<T, TSecond, TThird, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQueryOne<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = db.Query<T, TSecond, TThird, T>(
                    sql,
                    (first, second, third) =>
                        {
                            secondSetter(first, second);
                            thirdSetter(first, second, third);

                            return first;
                        },
                    parameters,
                    splitOn: splitOn);

                return result.SingleOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, object>> selector = null,
            Expression<Func<T, TSecond, TThird, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, T>(
                                     sql,
                                     (first, second, third) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.SingleOrDefault();
                }
            }
        }

        public virtual T QueryOne<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQueryOne<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = db.Query<T, TSecond, TThird, TFourth, T>(
                    sql,
                    (first, second, third, fourth) =>
                        {
                            secondSetter(first, second);
                            thirdSetter(first, second, third);
                            fourthSetter(first, second, third, fourth);

                            return first;
                        },
                    parameters,
                    splitOn: splitOn);

                return result.SingleOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, T>(
                                     sql,
                                     (first, second, third, fourth) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.SingleOrDefault();
                }
            }
        }

        public virtual T QueryOne<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQueryOne<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = db.Query<T, TSecond, TThird, TFourth, TFifth, T>(
                    sql,
                    (first, second, third, fourth, fifth) =>
                        {
                            secondSetter(first, second);
                            thirdSetter(first, second, third);
                            fourthSetter(first, second, third, fourth);
                            fifthSetter(first, second, third, fourth, fifth);

                            return first;
                        },
                    parameters,
                    splitOn: splitOn);

                return result.SingleOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, T>(
                                     sql,
                                     (first, second, third, fourth, fifth) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);
                                             fifthSetter(first, second, third, fourth, fifth);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.SingleOrDefault();
                }
            }
        }

        public virtual T QueryOne<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                sixthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQueryOne<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = db.Query<T, TSecond, TThird, TFourth, TFifth, TSixth, T>(
                    sql,
                    (first, second, third, fourth, fifth, sixth) =>
                        {
                            secondSetter(first, second);
                            thirdSetter(first, second, third);
                            fourthSetter(first, second, third, fourth);
                            fifthSetter(first, second, third, fourth, fifth);
                            sixthSetter(first, second, third, fourth, fifth, sixth);

                            return first;
                        },
                    parameters,
                    splitOn: splitOn);

                return result.SingleOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                sixthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, TSixth, T>(
                                     sql,
                                     (first, second, third, fourth, fifth, sixth) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);
                                             fifthSetter(first, second, third, fourth, fifth);
                                             sixthSetter(first, second, third, fourth, fifth, sixth);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.SingleOrDefault();
                }
            }
        }

        public virtual T QueryOne<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, seventhSetter) =
                this.GenerateQueryStatement(
                    this.tableName,
                    this.alias,
                    secondJoin,
                    thirdJoin,
                    fourthJoin,
                    fifthJoin,
                    sixthJoin,
                    seventhJoin,
                    predicate,
                    orderings,
                    selector,
                    groupingColumns,
                    groupingSelector,
                    skipped,
                    taken);

            if (groupingSelector != null) return this.ExecuteQueryOne<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = db.Query<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, T>(
                    sql,
                    (first, second, third, fourth, fifth, sixth, seventh) =>
                        {
                            secondSetter(first, second);
                            thirdSetter(first, second, third);
                            fourthSetter(first, second, third, fourth);
                            fifthSetter(first, second, third, fourth, fifth);
                            sixthSetter(first, second, third, fourth, fifth, sixth);
                            seventhSetter(first, second, third, fourth, fifth, sixth, seventh);

                            return first;
                        },
                    parameters,
                    splitOn: splitOn);

                return result.SingleOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, seventhSetter) =
                this.GenerateQueryStatement(
                    this.tableName,
                    this.alias,
                    secondJoin,
                    thirdJoin,
                    fourthJoin,
                    fifthJoin,
                    sixthJoin,
                    seventhJoin,
                    predicate,
                    orderings,
                    selector,
                    groupingColumns,
                    groupingSelector,
                    skipped,
                    taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, T>(
                                     sql,
                                     (first, second, third, fourth, fifth, sixth, seventh) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);
                                             fifthSetter(first, second, third, fourth, fifth);
                                             sixthSetter(first, second, third, fourth, fifth, sixth);
                                             seventhSetter(first, second, third, fourth, fifth, sixth, seventh);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.SingleOrDefault();
                }
            }
        }

        public virtual List<T> Query(string sql, object param)
        {
            return this.ExecuteQueryAsync<T>(sql, param).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public virtual Task<List<T>> QueryAsync(string sql, object param)
        {
            return this.ExecuteQueryAsync<T>(sql, param);
        }

        public virtual List<T> Query(
            Expression<Func<T, bool>> predicate,
            IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null,
            Expression<Func<T, object>> selector = null,
            Expression<Func<T, object>> groupingColumns = null,
            Expression<Func<Grouping<T>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters) = this.GenerateQueryStatement(this.tableName, this.alias, predicate, orderings, selector, groupingColumns, groupingSelector, skipped, taken);

            return this.ExecuteQuery<T>(sql, parameters);
        }

        public virtual Task<List<T>> QueryAsync(
            Expression<Func<T, bool>> predicate,
            IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null,
            Expression<Func<T, object>> selector = null,
            Expression<Func<T, object>> groupingColumns = null,
            Expression<Func<Grouping<T>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters) = this.GenerateQueryStatement(this.tableName, this.alias, predicate, orderings, selector, groupingColumns, groupingSelector, skipped, taken);

            return this.ExecuteQueryAsync<T>(sql, parameters);
        }

        public virtual List<T> Query<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, object>> selector = null,
            Expression<Func<T, TSecond, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQuery<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Query<T, TSecond, T>(
                        sql,
                        (first, second) =>
                            {
                                secondSetter(first, second);

                                return first;
                            },
                        parameters,
                        splitOn: splitOn)
                    .ToList();
            }
        }

        public virtual async Task<List<T>> QueryAsync<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, object>> selector = null,
            Expression<Func<T, TSecond, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, T>(
                                     sql,
                                     (first, second) =>
                                         {
                                             secondSetter(first, second);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.ToList();
                }
            }
        }

        public virtual List<T> Query<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, object>> selector = null,
            Expression<Func<T, TSecond, TThird, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQuery<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Query<T, TSecond, TThird, T>(
                        sql,
                        (first, second, third) =>
                            {
                                secondSetter(first, second);
                                thirdSetter(first, second, third);

                                return first;
                            },
                        parameters,
                        splitOn: splitOn)
                    .ToList();
            }
        }

        public virtual async Task<List<T>> QueryAsync<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, object>> selector = null,
            Expression<Func<T, TSecond, TThird, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, T>(
                                     sql,
                                     (first, second, third) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.ToList();
                }
            }
        }

        public virtual List<T> Query<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQuery<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Query<T, TSecond, TThird, TFourth, T>(
                        sql,
                        (first, second, third, fourth) =>
                            {
                                secondSetter(first, second);
                                thirdSetter(first, second, third);
                                fourthSetter(first, second, third, fourth);

                                return first;
                            },
                        parameters,
                        splitOn: splitOn)
                    .ToList();
            }
        }

        public virtual async Task<List<T>> QueryAsync<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, T>(
                                     sql,
                                     (first, second, third, fourth) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.ToList();
                }
            }
        }

        public virtual List<T> Query<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQuery<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Query<T, TSecond, TThird, TFourth, TFifth, T>(
                        sql,
                        (first, second, third, fourth, fifth) =>
                            {
                                secondSetter(first, second);
                                thirdSetter(first, second, third);
                                fourthSetter(first, second, third, fourth);
                                fifthSetter(first, second, third, fourth, fifth);

                                return first;
                            },
                        parameters,
                        splitOn: splitOn)
                    .ToList();
            }
        }

        public virtual async Task<List<T>> QueryAsync<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, T>(
                                     sql,
                                     (first, second, third, fourth, fifth) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);
                                             fifthSetter(first, second, third, fourth, fifth);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.ToList();
                }
            }
        }

        public virtual List<T> Query<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                sixthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQuery<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Query<T, TSecond, TThird, TFourth, TFifth, TSixth, T>(
                        sql,
                        (first, second, third, fourth, fifth, sixth) =>
                            {
                                secondSetter(first, second);
                                thirdSetter(first, second, third);
                                fourthSetter(first, second, third, fourth);
                                fifthSetter(first, second, third, fourth, fifth);
                                sixthSetter(first, second, third, fourth, fifth, sixth);

                                return first;
                            },
                        parameters,
                        splitOn: splitOn)
                    .ToList();
            }
        }

        public virtual async Task<List<T>> QueryAsync<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                sixthJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, TSixth, T>(
                                     sql,
                                     (first, second, third, fourth, fifth, sixth) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);
                                             fifthSetter(first, second, third, fourth, fifth);
                                             sixthSetter(first, second, third, fourth, fifth, sixth);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.ToList();
                }
            }
        }

        public virtual List<T> Query<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, seventhSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                sixthJoin,
                seventhJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null) return this.ExecuteQuery<T>(sql, parameters);

            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Query<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, T>(
                        sql,
                        (first, second, third, fourth, fifth, sixth, seventh) =>
                            {
                                secondSetter(first, second);
                                thirdSetter(first, second, third);
                                fourthSetter(first, second, third, fourth);
                                fifthSetter(first, second, third, fourth, fifth);
                                sixthSetter(first, second, third, fourth, fifth, sixth);
                                seventhSetter(first, second, third, fourth, fifth, sixth, seventh);

                                return first;
                            },
                        parameters,
                        splitOn: splitOn)
                    .ToList();
            }
        }

        public virtual async Task<List<T>> QueryAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, seventhSetter) = this.GenerateQueryStatement(
                this.tableName,
                this.alias,
                secondJoin,
                thirdJoin,
                fourthJoin,
                fifthJoin,
                sixthJoin,
                seventhJoin,
                predicate,
                orderings,
                selector,
                groupingColumns,
                groupingSelector,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                using (var db = new SqlConnection(this.connectionString))
                {
                    var result = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, T>(
                                     sql,
                                     (first, second, third, fourth, fifth, sixth, seventh) =>
                                         {
                                             secondSetter(first, second);
                                             thirdSetter(first, second, third);
                                             fourthSetter(first, second, third, fourth);
                                             fifthSetter(first, second, third, fourth, fifth);
                                             sixthSetter(first, second, third, fourth, fifth, sixth);
                                             seventhSetter(first, second, third, fourth, fifth, sixth, seventh);

                                             return first;
                                         },
                                     parameters,
                                     splitOn: splitOn);

                    return result.ToList();
                }
            }
        }

        public virtual int Count(Expression<Func<T, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(predicate);

            return this.ExecuteQueryOne<int>(sql, parameters);
        }

        public virtual Task<int> CountAsync(Expression<Func<T, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(predicate);

            return this.ExecuteQueryOneAsync<int>(sql, parameters);
        }

        public virtual int Count<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, predicate);

            return this.ExecuteQueryOne<int>(sql, parameters);
        }

        public virtual Task<int> CountAsync<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, predicate);

            return this.ExecuteQueryOneAsync<int>(sql, parameters);
        }

        public virtual int Count<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, predicate);

            return this.ExecuteQueryOne<int>(sql, parameters);
        }

        public virtual Task<int> CountAsync<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, predicate);

            return this.ExecuteQueryOneAsync<int>(sql, parameters);
        }

        public virtual int Count<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, predicate);

            return this.ExecuteQueryOne<int>(sql, parameters);
        }

        public virtual Task<int> CountAsync<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, predicate);

            return this.ExecuteQueryOneAsync<int>(sql, parameters);
        }

        public virtual int Count<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, fifthJoin, predicate);

            return this.ExecuteQueryOne<int>(sql, parameters);
        }

        public virtual Task<int> CountAsync<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, fifthJoin, predicate);

            return this.ExecuteQueryOneAsync<int>(sql, parameters);
        }

        public virtual int Count<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, fifthJoin, sixthJoin, predicate);

            return this.ExecuteQueryOne<int>(sql, parameters);
        }

        public virtual Task<int> CountAsync<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, fifthJoin, sixthJoin, predicate);

            return this.ExecuteQueryOneAsync<int>(sql, parameters);
        }

        public virtual int Count<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, fifthJoin, sixthJoin, seventhJoin, predicate);

            return this.ExecuteQueryOne<int>(sql, parameters);
        }

        public virtual Task<int> CountAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateCountStatement(secondJoin, thirdJoin, fourthJoin, fifthJoin, sixthJoin, seventhJoin, predicate);

            return this.ExecuteQueryOneAsync<int>(sql, parameters);
        }

        public virtual bool Exists(Expression<Func<T, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateExistsStatement(predicate);

            return this.ExecuteQueryOne<bool>(sql, parameters);
        }

        public virtual Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateExistsStatement(predicate);

            return this.ExecuteQueryOneAsync<bool>(sql, parameters);
        }

        public virtual int Execute(string sql, object param)
        {
            return this.ExecuteCommand(sql, param);
        }

        public virtual Task<int> ExecuteAsync(string sql, object param)
        {
            return this.ExecuteCommandAsync(sql, param);
        }

        public virtual int Insert(T value)
        {
            var sql = this.GenerateInsertStatement();

            return this.ExecuteCommand(sql, value);
        }

        public virtual Task<int> InsertAsync(T value)
        {
            var sql = this.GenerateInsertStatement();

            return this.ExecuteCommandAsync(sql, value);
        }

        public virtual int Insert(Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateInsertStatement(setter, true);

            return this.ExecuteCommand(sql, parameters);
        }

        public virtual Task<int> InsertAsync(Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateInsertStatement(setter, true);

            return this.ExecuteCommandAsync(sql, parameters);
        }

        public virtual int Insert(IEnumerable<T> values)
        {
            var sql = this.GenerateInsertStatement();

            return Transaction.Current != null ? this.ExecuteCommand(sql, values) : this.ExecuteTransactionalCommand(sql, values);
        }

        public virtual Task<int> InsertAsync(IEnumerable<T> values)
        {
            var sql = this.GenerateInsertStatement();

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual int Insert(Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateInsertStatement(setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommand(sql, values) : this.ExecuteTransactionalCommand(sql, values);
        }

        public virtual Task<int> InsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateInsertStatement(setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual int BulkInsert(IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(values);

            return this.ExecuteCommand(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual Task<int> BulkInsertAsync(IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(values);

            return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual int BulkInsert(Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(setterTemplate, values);

            return this.ExecuteCommand(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual Task<int> BulkInsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkInsertStatement(setterTemplate, values);

            return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual int Update(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateUpdateStatement(predicate, setter, true);

            return this.ExecuteCommand(sql, parameters);
        }

        public virtual Task<int> UpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateUpdateStatement(predicate, setter, true);

            return this.ExecuteCommandAsync(sql, parameters);
        }

        public virtual int Update(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateUpdateStatement(predicateTemplate, setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommand(sql, values) : this.ExecuteTransactionalCommand(sql, values);
        }

        public virtual Task<int> UpdateAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateUpdateStatement(predicateTemplate, setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual int BulkUpdate(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkUpdateStatement(predicateTemplate, setterTemplate, values);

            return this.ExecuteCommand(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual Task<int> BulkUpdateAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkUpdateStatement(predicateTemplate, setterTemplate, values);

            return this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual int Upsert(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateUpsertStatement(predicate, setter, true);

            return Transaction.Current != null ? this.ExecuteCommand(sql, parameters) : this.ExecuteTransactionalCommand(sql, parameters);
        }

        public virtual Task<int> UpsertAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter)
        {
            var (sql, parameters) = this.GenerateUpsertStatement(predicate, setter, true);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, parameters) : this.ExecuteTransactionalCommandAsync(sql, parameters);
        }

        public virtual int Upsert(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateUpsertStatement(predicateTemplate, setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommand(sql, values) : this.ExecuteTransactionalCommand(sql, values);
        }

        public virtual Task<int> UpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, _) = this.GenerateUpsertStatement(predicateTemplate, setterTemplate, false);

            return Transaction.Current != null ? this.ExecuteCommandAsync(sql, values) : this.ExecuteTransactionalCommandAsync(sql, values);
        }

        public virtual int BulkUpsert(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkUpsertStatement(predicateTemplate, setterTemplate, values);

            return Transaction.Current != null
                       ? this.ExecuteCommand(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) })
                       : this.ExecuteTransactionalCommand(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual Task<int> BulkUpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (sql, tableType, tableVariable) = this.GenerateBulkUpsertStatement(predicateTemplate, setterTemplate, values);

            return Transaction.Current != null
                       ? this.ExecuteCommandAsync(sql, new { TableVariable = tableVariable.AsTableValuedParameter(tableType) })
                       : this.ExecuteTransactionalCommandAsync(sql,  new { TableVariable = tableVariable.AsTableValuedParameter(tableType) });
        }

        public virtual int Delete(Expression<Func<T, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateDeleteStatement(predicate);

            return this.ExecuteCommand(sql, parameters);
        }

        public virtual Task<int> DeleteAsync(Expression<Func<T, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateDeleteStatement(predicate);

            return this.ExecuteCommandAsync(sql, parameters);
        }

        protected virtual TResult ExecuteQueryOne<TResult>(string sql, object param)
        {
            using (var db = new SqlConnection(this.connectionString))
            {
                return db.QuerySingleOrDefault<TResult>(sql, param);
            }
        }

        protected virtual async Task<TResult> ExecuteQueryOneAsync<TResult>(string sql, object param)
        {
            using (var db = new SqlConnection(this.connectionString))
            {
                var result = await db.QuerySingleOrDefaultAsync<TResult>(sql, param);

                return result;
            }
        }

        protected virtual List<TResult> ExecuteQuery<TResult>(string sql, object param)
        {
            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Query<TResult>(sql, param).ToList();
            }
        }

        protected virtual async Task<List<TResult>> ExecuteQueryAsync<TResult>(string sql, object param)
        {
            using (var db = new SqlConnection(this.connectionString))
            {
                var result = await db.QueryAsync<TResult>(sql, param);

                return result.ToList();
            }
        }

        protected virtual int ExecuteCommand(string sql, object param)
        {
            using (var db = new SqlConnection(this.connectionString))
            {
                return db.Execute(sql, param);
            }
        }

        protected virtual async Task<int> ExecuteCommandAsync(string sql, object param)
        {
            using (var db = new SqlConnection(this.connectionString))
            {
                var result = await db.ExecuteAsync(sql, param);

                return result;
            }
        }

        protected virtual int ExecuteTransactionalCommand(string sql, object param)
        {
            int result;
            using (var db = new SqlConnection(this.connectionString))
            {
                db.Open();

                using (var tx = db.BeginTransaction())
                {
                    try
                    {
                        result = db.Execute(sql, param, transaction: tx);

                        tx.Commit();
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }

            return result;
        }

        protected virtual async Task<int> ExecuteTransactionalCommandAsync(string sql, object param)
        {
            int result;
            using (var db = new SqlConnection(this.connectionString))
            {
                await db.OpenAsync();

                using (var tx = db.BeginTransaction())
                {
                    try
                    {
                        result = await db.ExecuteAsync(sql, param, transaction: tx);

                        tx.Commit();
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }

            return result;
        }

        private static string GenerateAlias(Type type, int suffixNo)
        {
            var alias = Regex.Replace(type.Name, "[^A-Z]", string.Empty).ToLower();

            if (string.IsNullOrEmpty(alias) || alias.Length < 2) alias = type.Name.Left(3).ToLower();

            alias = string.Concat(alias, "_", suffixNo.ToString());

            return alias;
        }

        private static Action<T, TSecond> GetOrCreateSetter<TSecond>(Expression<Func<T, TSecond>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ") -> [0]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[] { instanceParam, argumentParam };

                        return Expression.Lambda<Action<T, TSecond>>(
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                                parameters)
                            .Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird> GetOrCreateSetter<TSecond, TThird>(Expression<Func<T, TSecond, TThird>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentParam
                                         };

                        return Expression.Lambda<Action<T, TSecond, TThird>>(
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                                parameters)
                            .Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth> GetOrCreateSetter<TSecond, TThird, TFourth>(Expression<Func<T, TSecond, TThird, TFourth>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentParam
                                         };

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth>>(
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                                parameters)
                            .Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth>(Expression<Func<T, TSecond, TThird, TFourth, TFifth>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentParam
                                         };

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth>>(
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                                parameters)
                            .Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth, TSixth> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth, TSixth>(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ", ", typeof(TSixth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth, TSixth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentIndex == 4 ? instanceParam : Expression.Parameter(typeof(TFifth)),
                                             argumentParam
                                         };

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth, TSixth>>(
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                                parameters)
                            .Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ", ", typeof(TSixth).FullName, ", ", typeof(TSeventh).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentIndex == 4 ? instanceParam : Expression.Parameter(typeof(TFifth)),
                                             argumentIndex == 5 ? instanceParam : Expression.Parameter(typeof(TSixth)),
                                             argumentParam
                                         };

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>(
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                                parameters)
                            .Compile();
                    });

            return setter;
        }

        private static (string, string) ResolveColumnList(string sql)
        {
            var columnList = new Dictionary<string, string>();

            foreach (var match in ColumnValueRegex.Matches(sql).Cast<Match>())
            {
                if (columnList.ContainsKey(match.Groups[1].Value)) continue;

                columnList.Add(match.Groups[1].Value, match.Groups[3].Value);
            }

            return (string.Join(", ", columnList.Keys), string.Join(", ", columnList.Values));
        }

        private (string, IDictionary<string, object>) GenerateQueryStatement(
            string tableName,
            string alias,
            Expression<Func<T, bool>> predicate,
            IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null,
            Expression<Func<T, object>> selector = null,
            Expression<Func<T, object>> groupingColumns = null,
            Expression<Func<Grouping<T>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            SqlBuilder sql = $@"
SELECT {(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(new[] { alias });
            }
            else if (selector != null)
            {
                sql += selector.ToSelectList(alias);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(alias, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(new[] { alias });

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? string.Empty : alias);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";


            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>) GenerateQueryStatement<TSecond>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, object>> selector = null,
            Expression<Func<T, TSecond, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2) };

            SqlBuilder sql = $@"
SELECT {(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = GetOrCreateSetter(secondJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>) GenerateQueryStatement<TSecond, TThird>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, object>> selector = null,
            Expression<Func<T, TSecond, TThird, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3) };

            SqlBuilder sql = $@"
SELECT {(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = GetOrCreateSetter(thirdJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>) GenerateQueryStatement<TSecond, TThird, TFourth>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4) };

            SqlBuilder sql = $@"
SELECT {(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = GetOrCreateSetter(fourthJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>, Action<T, TSecond, TThird, TFourth, TFifth>) GenerateQueryStatement<TSecond, TThird, TFourth, TFifth>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4), GenerateAlias(typeof(TFifth), 5) };

            SqlBuilder sql = $@"
SELECT {(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item2, fifthJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = GetOrCreateSetter(fourthJoin.Item1);
            var fifthSetter = GetOrCreateSetter(fifthJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>, Action<T, TSecond, TThird, TFourth, TFifth>, Action<T, TSecond, TThird, TFourth, TFifth, TSixth>) GenerateQueryStatement<TSecond, TThird, TFourth, TFifth, TSixth>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4), GenerateAlias(typeof(TFifth), 5), GenerateAlias(typeof(TSixth), 6) };

            SqlBuilder sql = $@"
SELECT {(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item2, fifthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item2, sixthJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = GetOrCreateSetter(fourthJoin.Item1);
            var fifthSetter = GetOrCreateSetter(fifthJoin.Item1);
            var sixthSetter = GetOrCreateSetter(sixthJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>, Action<T, TSecond, TThird, TFourth, TFifth>, Action<T, TSecond, TThird, TFourth, TFifth, TSixth>, Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>) GenerateQueryStatement<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4), GenerateAlias(typeof(TFifth), 5), GenerateAlias(typeof(TSixth), 6), GenerateAlias(typeof(TSeventh), 7) };

            SqlBuilder sql = $@"
SELECT {(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item2, fifthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item2, sixthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TSeventh>(seventhJoin.Item2, seventhJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = GetOrCreateSetter(fourthJoin.Item1);
            var fifthSetter = GetOrCreateSetter(fifthJoin.Item1);
            var sixthSetter = GetOrCreateSetter(sixthJoin.Item1);
            var seventhSetter = GetOrCreateSetter(seventhJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, seventhSetter);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement(Expression<Func<T, bool>> predicate)
        {
            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(this.alias, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate)
        {
            var aliases = new[] { this.alias, GenerateAlias(typeof(TSecond), 2) };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate)
        {
            var aliases = new[] { this.alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3) };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item2, fifthJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5),
                              GenerateAlias(typeof(TSixth), 6)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item2, fifthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item2, sixthJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5),
                              GenerateAlias(typeof(TSixth), 6),
                              GenerateAlias(typeof(TSeventh), 7)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item2, secondJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item2, thirdJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item2, fourthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item2, fifthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item2, sixthJoin.Item3, aliases, parameters);
            sql += this.GenerateJoinStatement<TSeventh>(seventhJoin.Item2, seventhJoin.Item3, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement(Expression<Func<T, bool>> predicate)
        {
            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(this.alias, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private string GenerateInsertStatement()
        {
            var requiredColumns = RequiredColumns.GetOrAdd(
                typeof(T),
                type => type.GetProperties().Where(p => Attribute.IsDefined(p, typeof(RequiredAttribute))).ToArray());

            if (requiredColumns.Length == 0) throw new ArgumentException("There must be at least one [Required] column.");

            var columnList = requiredColumns.ToColumnList(out var valueList);

            var sql = $@"
INSERT INTO [{this.tableName}]({columnList})
    VALUES ({valueList});";

            this.OutputSql?.Invoke(sql, null);

            return sql;
        }

        private (string, IDictionary<string, object>) GenerateInsertStatement(Expression<Func<T>> setter, bool outParameters)
        {
            string valueList;
            IDictionary<string, object> parameters = null;

            var columnList = outParameters ? setter.ToColumnList(out valueList, out parameters) : setter.ToColumnList(out valueList);

            var sql = $@"
INSERT INTO [{this.tableName}]({columnList})
    VALUES ({valueList});";

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, string, DataTable) GenerateBulkInsertStatement(IEnumerable<T> values)
        {
            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values);

            var requiredColumns = RequiredColumns.GetOrAdd(
                typeof(T),
                type => type.GetProperties().Where(p => Attribute.IsDefined(p, typeof(RequiredAttribute))).ToArray());

            if (requiredColumns.Length == 0) throw new ArgumentException("There must be at least one [Required] column.");

            var columnList = requiredColumns.ToColumnList(out _);

            var sql = $@"
INSERT INTO [{this.tableName}]({columnList})
    SELECT {ColumnRegex.Replace(columnList, "$0 = tvp.$0")}
    FROM @TableVariable tvp;";

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, string, DataTable) GenerateBulkInsertStatement(Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values);

            var columnList = setterTemplate.ToColumnList(out _);

            var sql = $@"
INSERT INTO [{this.tableName}]({columnList})
    SELECT {ColumnRegex.Replace(columnList, "$0 = tvp.$0")}
    FROM @TableVariable tvp;";

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, IDictionary<string, object>) GenerateUpdateStatement(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter, bool outParameters)
        {
            IDictionary<string, object> parameters = null;

            SqlBuilder sql = $@"
UPDATE [{this.tableName}]
SET ";
            sql += outParameters ? setter.ToSetStatements(out parameters) : setter.ToSetStatements();
            sql += @"
WHERE ";
            sql += outParameters ? predicate.ToSearchCondition(parameters) : predicate.ToSearchCondition();
            sql += ";";

            this.OutputSql?.Invoke(sql, null);

            return (sql, parameters);
        }

        private (string, string, DataTable) GenerateBulkUpdateStatement(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values);

            var columnList = setterTemplate.ToColumnList(out _);
            var searchCondition = predicateTemplate.ToSearchCondition();

            var sql = $@"
UPDATE [{this.tableName}]
SET {ColumnRegex.Replace(columnList, "$0 = tvp.$0")}
FROM [{this.tableName}] t
INNER JOIN @TableVariable tvp
    ON {ColumnValueRegex.Replace(searchCondition, "t.$1 = tvp.$1")};";

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, IDictionary<string, object>) GenerateUpsertStatement(
            Expression<Func<T, bool>> predicate,
            Expression<Func<T>> setter,
            bool outParameters)
        {
            IDictionary<string, object> parameters = null;

            SqlBuilder sql = $@"
UPDATE [{this.tableName}]
SET ";
            sql += outParameters ? setter.ToSetStatements(out parameters) : setter.ToSetStatements();
            sql += @"
WHERE ";
            sql += outParameters ? predicate.ToSearchCondition(parameters) : predicate.ToSearchCondition();
            sql += ";";

            var (columnList, valueList) = ResolveColumnList(sql);

            sql.Append("\r\n");
            sql += $@"
IF @@rowcount = 0
    BEGIN
        INSERT INTO [{this.tableName}]({columnList})
            VALUES ({valueList});
    END";

            this.OutputSql?.Invoke(sql, null);

            return (sql, parameters);
        }

        private (string, string, DataTable) GenerateBulkUpsertStatement(
            Expression<Func<T, bool>> predicateTemplate,
            Expression<Func<T>> setterTemplate,
            IEnumerable<T> values)
        {
            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values);

            var columnList = setterTemplate.ToColumnList(out _);
            var searchCondition = predicateTemplate.ToSearchCondition();

            SqlBuilder sql = $@"
UPDATE [{this.tableName}]
SET {ColumnRegex.Replace(columnList, "$0 = tvp.$0")}
FROM [{this.tableName}] t
INNER JOIN @TableVariable tvp
    ON {ColumnValueRegex.Replace(searchCondition, "t.$1 = tvp.$1")};";

            (columnList, _) = ResolveColumnList(sql);

            sql.Append("\r\n");
            sql += $@"
INSERT INTO [{this.tableName}]({columnList})
    SELECT {ColumnRegex.Replace(columnList, "tvp.$0")}
    FROM @TableVariable tvp
    WHERE NOT EXISTS (SELECT
                1
            FROM [{this.tableName}] t WITH (NOLOCK)
            WHERE {ColumnValueRegex.Replace(searchCondition, "t.$1 = tvp.$1")});";

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, IDictionary<string, object>) GenerateDeleteStatement(Expression<Func<T, bool>> predicate)
        {
            SqlBuilder sql = $@"
DELETE FROM [{this.tableName}]
WHERE ";
            sql += predicate.ToSearchCondition(out var parameters);

            this.OutputSql?.Invoke(sql, null);

            return (sql, parameters);
        }

        private string GenerateJoinStatement<TRight>(LambdaExpression condition, JoinType joinType, string[] aliases, IDictionary<string, object> parameters)
        {
            if (condition == null)
            {
                throw new ArgumentException("Must have join condition.");
            }

            var server = ServerRegex.Match(this.connectionString).Groups[2].Value.Trim();
            var database = DatabaseRegex.Match(this.connectionString).Groups[2].Value.Trim();
            var rightServer = server;
            var rightDatabase = database;
            var rightSchema = "dbo";

            foreach (var connectionStringAttr in typeof(TRight).GetCustomAttributes<ConnectionStringAttribute>())
            {
                var rightConnectionString = SqlServerDataAccessFactory.Instance.GetConnectionString(connectionStringAttr.ConnectionString);

                rightServer = ServerRegex.Match(rightConnectionString).Groups[2].Value.Trim();
                rightDatabase = DatabaseRegex.Match(rightConnectionString).Groups[2].Value.Trim();
                rightSchema = connectionStringAttr.Schema;

                if (string.Equals(server, rightServer, StringComparison.CurrentCultureIgnoreCase)) break;
            }

            if (!string.Equals(server, rightServer, StringComparison.CurrentCultureIgnoreCase))
            {
                throw new ArgumentException("Table is not in the same database server.");
            }

            switch (joinType)
            {
                case JoinType.Inner:
                    return string.Equals(database, rightDatabase, StringComparison.CurrentCultureIgnoreCase)
                               ? string.Concat("\r\n", condition.ToInnerJoin<TRight>(aliases, null, null, parameters))
                               : string.Concat("\r\n", condition.ToInnerJoin<TRight>(aliases, rightDatabase, rightSchema, parameters));

                case JoinType.Left:
                    return string.Equals(database, rightDatabase, StringComparison.CurrentCultureIgnoreCase)
                               ? string.Concat("\r\n", condition.ToLeftJoin<TRight>(aliases, null, null, parameters))
                               : string.Concat("\r\n", condition.ToLeftJoin<TRight>(aliases, rightDatabase, rightSchema, parameters));

                default: throw new ArgumentOutOfRangeException(nameof(joinType), "Unsupported join type.");
            }
        }

        private (string, DataTable) ConvertToTableValuedParameters(IEnumerable<T> values)
        {
            var userDefinedAttribute = typeof(T).GetCustomAttribute<UserDefinedAttribute>(true);

            if (userDefinedAttribute == null) throw new ArgumentException("Must has UserDefinedAttribute.");

            var tableType = userDefinedAttribute.TableType;

            var columns = SqlServerDataAccessFactory.Instance.GetUserDefinedTable(tableType);

            if (string.IsNullOrEmpty(tableType) || columns == null)
            {
                throw new ArgumentException("Must configure user-defined table type.");
            }

            var dataTable = new DataTable();

            dataTable.Columns.AddRange(columns.ToArray());

            var properties = columns.ToDictionary(
                x => x.ColumnName,
                x =>
                    {
                        var property = typeof(T).GetProperty(x.ColumnName);

                        if (property == null)
                        {
                            property = typeof(T).GetProperties().Single(p => p.GetCustomAttribute<ColumnAttribute>()?.Name == x.ColumnName);
                        }

                        return property;
                    });

            foreach (var value in values)
            {
                var dataRow = dataTable.NewRow();

                foreach (var dataColumn in columns)
                {
                    dataRow[dataColumn.ColumnName] = properties[dataColumn.ColumnName].GetValue(value);
                }

                dataTable.Rows.Add(dataRow);
            }

            return (tableType, dataTable);
        }
    }
}