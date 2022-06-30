using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Dapper;

namespace Chef.DbAccess.SqlServer
{
    public partial class SqlServerDataAccess<T>
    {
        public virtual Task<T> QueryOneAsync(string sql, object param)
        {
            return this.ExecuteQueryOneAsync<T>(sql, param);
        }

        public virtual Task<T> QueryOneAsync(
            Expression<Func<T, bool>> predicate,
            IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null,
            Expression<Func<T, object>> selector = null,
            Expression<Func<T, object>> groupingColumns = null,
            Expression<Func<Grouping<T>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            var (sql, parameters) = this.GenerateQueryStatement(this.tableName, this.alias, predicate, orderings, selector, groupingColumns, groupingSelector, distinct, skipped, taken);

            return this.ExecuteQueryOneAsync<T>(sql, parameters);
        }

        public virtual async Task<T> QueryOneAsync<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, object>> selector = null,
            Expression<Func<T, TSecond, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null,
            bool distinct = false,
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
                distinct,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                var firstDict = new Dictionary<T, T>();
                var secondDict = new Dictionary<TSecond, TSecond>();

                using (var db = new SqlConnection(this.connectionString))
                {
                    _ = await db.QueryAsync<T, TSecond, T>(
                            sql,
                            (first, second) =>
                                {
                                    if (!firstDict.TryGetValue(first, out var outFirst))
                                    {
                                        firstDict.Add(first, outFirst = first);
                                    }

                                    if (firstDict.Count > 1) throw new InvalidOperationException("查詢結果超過一筆");

                                    var outSecond = default(TSecond);

                                    if (second != null && !secondDict.TryGetValue(second, out outSecond))
                                    {
                                        secondDict.Add(second, outSecond = second);
                                    }

                                    secondSetter(outFirst, outSecond);

                                    return outFirst;
                                },
                            parameters,
                            splitOn: splitOn);
                }

                return firstDict.Values.FirstOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, object>> selector = null,
            Expression<Func<T, TSecond, TThird, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null,
            bool distinct = false,
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
                distinct,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                var firstDict = new Dictionary<T, T>();
                var secondDict = new Dictionary<TSecond, TSecond>();
                var thirdDict = new Dictionary<TThird, TThird>();

                using (var db = new SqlConnection(this.connectionString))
                {
                    _ = await db.QueryAsync<T, TSecond, TThird, T>(
                            sql,
                            (first, second, third) =>
                                {
                                    if (!firstDict.TryGetValue(first, out var outFirst))
                                    {
                                        firstDict.Add(first, outFirst = first);
                                    }

                                    if (firstDict.Count > 1) throw new InvalidOperationException("查詢結果超過一筆");

                                    var outSecond = default(TSecond);

                                    if (second != null && !secondDict.TryGetValue(second, out outSecond))
                                    {
                                        secondDict.Add(second, outSecond = second);
                                    }

                                    var outThird = default(TThird);

                                    if (third != null && !thirdDict.TryGetValue(third, out outThird))
                                    {
                                        thirdDict.Add(third, outThird = third);
                                    }

                                    secondSetter(outFirst, outSecond);
                                    thirdSetter(outFirst, outSecond, outThird);

                                    return first;
                                },
                            parameters,
                            splitOn: splitOn);
                }

                return firstDict.Values.FirstOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null,
            bool distinct = false,
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
                distinct,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                var firstDict = new Dictionary<T, T>();
                var secondDict = new Dictionary<TSecond, TSecond>();
                var thirdDict = new Dictionary<TThird, TThird>();
                var fourthDict = new Dictionary<TFourth, TFourth>();

                using (var db = new SqlConnection(this.connectionString))
                {
                    _ = await db.QueryAsync<T, TSecond, TThird, TFourth, T>(
                            sql,
                            (first, second, third, fourth) =>
                                {
                                    if (!firstDict.TryGetValue(first, out var outFirst))
                                    {
                                        firstDict.Add(first, outFirst = first);
                                    }

                                    if (firstDict.Count > 1) throw new InvalidOperationException("查詢結果超過一筆");

                                    var outSecond = default(TSecond);

                                    if (second != null && !secondDict.TryGetValue(second, out outSecond))
                                    {
                                        secondDict.Add(second, outSecond = second);
                                    }

                                    var outThird = default(TThird);

                                    if (third != null && !thirdDict.TryGetValue(third, out outThird))
                                    {
                                        thirdDict.Add(third, outThird = third);
                                    }

                                    var outFourth = default(TFourth);

                                    if (fourth != null && !fourthDict.TryGetValue(fourth, out outFourth))
                                    {
                                        fourthDict.Add(fourth, outFourth = fourth);
                                    }

                                    secondSetter(outFirst, outSecond);
                                    thirdSetter(outFirst, outSecond, outThird);
                                    fourthSetter(outFirst, outSecond, outThird, outFourth);

                                    return first;
                                },
                            parameters,
                            splitOn: splitOn);
                }

                return firstDict.Values.FirstOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null,
            bool distinct = false,
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
                distinct,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                var firstDict = new Dictionary<T, T>();
                var secondDict = new Dictionary<TSecond, TSecond>();
                var thirdDict = new Dictionary<TThird, TThird>();
                var fourthDict = new Dictionary<TFourth, TFourth>();
                var fifthDict = new Dictionary<TFifth, TFifth>();

                using (var db = new SqlConnection(this.connectionString))
                {
                    _ = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, T>(
                            sql,
                            (first, second, third, fourth, fifth) =>
                                {
                                    if (!firstDict.TryGetValue(first, out var outFirst))
                                    {
                                        firstDict.Add(first, outFirst = first);
                                    }

                                    if (firstDict.Count > 1) throw new InvalidOperationException("查詢結果超過一筆");

                                    var outSecond = default(TSecond);

                                    if (second != null && !secondDict.TryGetValue(second, out outSecond))
                                    {
                                        secondDict.Add(second, outSecond = second);
                                    }

                                    var outThird = default(TThird);

                                    if (third != null && !thirdDict.TryGetValue(third, out outThird))
                                    {
                                        thirdDict.Add(third, outThird = third);
                                    }

                                    var outFourth = default(TFourth);

                                    if (fourth != null && !fourthDict.TryGetValue(fourth, out outFourth))
                                    {
                                        fourthDict.Add(fourth, outFourth = fourth);
                                    }

                                    var outFifth = default(TFifth);

                                    if (fifth != null && !fifthDict.TryGetValue(fifth, out outFifth))
                                    {
                                        fifthDict.Add(fifth, outFifth = fifth);
                                    }

                                    secondSetter(outFirst, outSecond);
                                    thirdSetter(outFirst, outSecond, outThird);
                                    fourthSetter(outFirst, outSecond, outThird, outFourth);
                                    fifthSetter(outFirst, outSecond, outThird, outFourth, outFifth);

                                    return first;
                                },
                            parameters,
                            splitOn: splitOn);
                }

                return firstDict.Values.FirstOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null,
            bool distinct = false,
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
                distinct,
                skipped,
                taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                var firstDict = new Dictionary<T, T>();
                var secondDict = new Dictionary<TSecond, TSecond>();
                var thirdDict = new Dictionary<TThird, TThird>();
                var fourthDict = new Dictionary<TFourth, TFourth>();
                var fifthDict = new Dictionary<TFifth, TFifth>();
                var sixthDict = new Dictionary<TSixth, TSixth>();

                using (var db = new SqlConnection(this.connectionString))
                {
                    _ = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, TSixth, T>(
                            sql,
                            (first, second, third, fourth, fifth, sixth) =>
                                {
                                    if (!firstDict.TryGetValue(first, out var outFirst))
                                    {
                                        firstDict.Add(first, outFirst = first);
                                    }

                                    if (firstDict.Count > 1) throw new InvalidOperationException("查詢結果超過一筆");

                                    var outSecond = default(TSecond);

                                    if (second != null && !secondDict.TryGetValue(second, out outSecond))
                                    {
                                        secondDict.Add(second, outSecond = second);
                                    }

                                    var outThird = default(TThird);

                                    if (third != null && !thirdDict.TryGetValue(third, out outThird))
                                    {
                                        thirdDict.Add(third, outThird = third);
                                    }

                                    var outFourth = default(TFourth);

                                    if (fourth != null && !fourthDict.TryGetValue(fourth, out outFourth))
                                    {
                                        fourthDict.Add(fourth, outFourth = fourth);
                                    }

                                    var outFifth = default(TFifth);

                                    if (fifth != null && !fifthDict.TryGetValue(fifth, out outFifth))
                                    {
                                        fifthDict.Add(fifth, outFifth = fifth);
                                    }

                                    var outSixth = default(TSixth);

                                    if (sixth != null && !sixthDict.TryGetValue(sixth, out outSixth))
                                    {
                                        sixthDict.Add(sixth, outSixth = sixth);
                                    }

                                    secondSetter(outFirst, outSecond);
                                    thirdSetter(outFirst, outSecond, outThird);
                                    fourthSetter(outFirst, outSecond, outThird, outFourth);
                                    fifthSetter(outFirst, outSecond, outThird, outFourth, outFifth);
                                    sixthSetter(outFirst, outSecond, outThird, outFourth, outFifth, outSixth);

                                    return first;
                                },
                            parameters,
                            splitOn: splitOn);
                }

                return firstDict.Values.FirstOrDefault();
            }
        }

        public virtual async Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null,
            bool distinct = false,
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
                    distinct,
                    skipped,
                    taken);

            if (groupingSelector != null)
            {
                var result = await this.ExecuteQueryOneAsync<T>(sql, parameters);

                return result;
            }
            else
            {
                var firstDict = new Dictionary<T, T>();
                var secondDict = new Dictionary<TSecond, TSecond>();
                var thirdDict = new Dictionary<TThird, TThird>();
                var fourthDict = new Dictionary<TFourth, TFourth>();
                var fifthDict = new Dictionary<TFifth, TFifth>();
                var sixthDict = new Dictionary<TSixth, TSixth>();
                var seventhDict = new Dictionary<TSeventh, TSeventh>();

                using (var db = new SqlConnection(this.connectionString))
                {
                    _ = await db.QueryAsync<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, T>(
                            sql,
                            (first, second, third, fourth, fifth, sixth, seventh) =>
                                {
                                    if (!firstDict.TryGetValue(first, out var outFirst))
                                    {
                                        firstDict.Add(first, outFirst = first);
                                    }

                                    if (firstDict.Count > 1) throw new InvalidOperationException("查詢結果超過一筆");

                                    var outSecond = default(TSecond);

                                    if (second != null && !secondDict.TryGetValue(second, out outSecond))
                                    {
                                        secondDict.Add(second, outSecond = second);
                                    }

                                    var outThird = default(TThird);

                                    if (third != null && !thirdDict.TryGetValue(third, out outThird))
                                    {
                                        thirdDict.Add(third, outThird = third);
                                    }

                                    var outFourth = default(TFourth);

                                    if (fourth != null && !fourthDict.TryGetValue(fourth, out outFourth))
                                    {
                                        fourthDict.Add(fourth, outFourth = fourth);
                                    }

                                    var outFifth = default(TFifth);

                                    if (fifth != null && !fifthDict.TryGetValue(fifth, out outFifth))
                                    {
                                        fifthDict.Add(fifth, outFifth = fifth);
                                    }

                                    var outSixth = default(TSixth);

                                    if (sixth != null && !sixthDict.TryGetValue(sixth, out outSixth))
                                    {
                                        sixthDict.Add(sixth, outSixth = sixth);
                                    }

                                    var outSeventh = default(TSeventh);

                                    if (seventh != null && !seventhDict.TryGetValue(seventh, out outSeventh))
                                    {
                                        seventhDict.Add(seventh, outSeventh = seventh);
                                    }

                                    secondSetter(outFirst, outSecond);
                                    thirdSetter(outFirst, outSecond, outThird);
                                    fourthSetter(outFirst, outSecond, outThird, outFourth);
                                    fifthSetter(outFirst, outSecond, outThird, outFourth, outFifth);
                                    sixthSetter(outFirst, outSecond, outThird, outFourth, outFifth, outSixth);
                                    seventhSetter(outFirst, outSecond, outThird, outFourth, outFifth, outSixth, outSeventh);

                                    return first;
                                },
                            parameters,
                            splitOn: splitOn);
                }

                return firstDict.Values.FirstOrDefault();
            }
        }
    }
}