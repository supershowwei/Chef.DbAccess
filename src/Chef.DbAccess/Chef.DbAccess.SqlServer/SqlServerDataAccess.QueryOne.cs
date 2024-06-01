using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

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
                var result = await this.ExecuteQueryOneAsync(sql, secondSetter, parameters, splitOn);

                return result;
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
                var result = await this.ExecuteQueryOneAsync(sql, secondSetter, thirdSetter, parameters, splitOn);

                return result;
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
                var result = await this.ExecuteQueryOneAsync(sql, secondSetter, thirdSetter, fourthSetter, parameters, splitOn);

                return result;
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
                var result = await this.ExecuteQueryOneAsync(sql, secondSetter, thirdSetter, fourthSetter, fifthSetter, parameters, splitOn);

                return result;
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
                var result = await this.ExecuteQueryOneAsync(sql, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, parameters, splitOn);

                return result;
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
                var result = await this.ExecuteQueryOneAsync(sql, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, seventhSetter, parameters, splitOn);

                return result;
            }
        }
    }
}