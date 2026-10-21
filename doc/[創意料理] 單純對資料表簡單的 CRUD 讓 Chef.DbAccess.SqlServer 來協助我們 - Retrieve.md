[創意料理] 單純對資料表簡單的 CRUD 讓 Chef.DbAccess.SqlServer 來協助我們 - Retrieve

## R - Retrieve

關於查詢資料的方法有 30 個如下：

    public interface IDataAccess<T>
    {
        ..
        Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate);
        Task<bool> ExistsAsync<TSecond>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, Expression<Func<T, TSecond, bool>> predicate);
        Task<bool> ExistsAsync<TSecond, TThird>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, Expression<Func<T, TSecond, TThird, bool>> predicate);
        Task<bool> ExistsAsync<TSecond, TThird, TFourth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate);
        Task<bool> ExistsAsync<TSecond, TThird, TFourth, TFifth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate);
        Task<bool> ExistsAsync<TSecond, TThird, TFourth, TFifth, TSixth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate);
        Task<bool> ExistsAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate);
        Task<int> CountAsync<TSecond>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, Expression<Func<T, TSecond, bool>> predicate);
        Task<int> CountAsync<TSecond, TThird>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, Expression<Func<T, TSecond, TThird, bool>> predicate);
        Task<int> CountAsync<TSecond, TThird, TFourth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate);
        Task<int> CountAsync<TSecond, TThird, TFourth, TFifth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate);
        Task<int> CountAsync<TSecond, TThird, TFourth, TFifth, TSixth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate);
        Task<int> CountAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate);
        Task<int> CountAsync(Expression<Func<T, bool>> predicate);
        Task<T> QueryOneAsync(Expression<Func<T, bool>> predicate, IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null, Expression<Func<T, object>> selector = null, Expression<Func<T, object>> groupingColumns = null, Expression<Func<Grouping<T>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<T> QueryOneAsync<TSecond>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, Expression<Func<T, TSecond, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, object>> selector = null, Expression<Func<T, TSecond, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<T> QueryOneAsync<TSecond, TThird>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, Expression<Func<T, TSecond, TThird, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, object>> selector = null, Expression<Func<T, TSecond, TThird, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<T> QueryOneAsync<TSecond, TThird, TFourth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<T> QueryOneAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<T> QueryOneAsync(string sql, object param);
        Task<List<T>> QueryAsync(Expression<Func<T, bool>> predicate, IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null, Expression<Func<T, object>> selector = null, Expression<Func<T, object>> groupingColumns = null, Expression<Func<Grouping<T>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<List<T>> QueryAsync<TSecond>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, Expression<Func<T, TSecond, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, object>> selector = null, Expression<Func<T, TSecond, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<List<T>> QueryAsync<TSecond, TThird>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, Expression<Func<T, TSecond, TThird, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, object>> selector = null, Expression<Func<T, TSecond, TThird, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<List<T>> QueryAsync<TSecond, TThird, TFourth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<List<T>> QueryAsync<TSecond, TThird, TFourth, TFifth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<List<T>> QueryAsync<TSecond, TThird, TFourth, TFifth, TSixth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<List<T>> QueryAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate, IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null, Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null, bool distinct = false, int? skipped = null, int? taken = null);
        Task<List<T>> QueryAsync(string sql, object param);
        ...
    }

查詢語法目前除了必要的 `SELECT ... FROM ... WHERE` 之外，還支援底下幾個語法：

- 比較運算子：`=`、`<>`、`>`、`<`、`>=`、`<=`、`LIKE`、`NOT LIKE`、`IN`、`NOT IN`、`IS NULL`、`IS NOT NULL`
- 邏輯運算子：`&`、`|`
- 字串欄位比大小：`x.Name.CompareTo("strB") [>, >=, <, <=] 0`。
- `DISTINCT`
- `TOP`
- `ORDER BY`
- `GROUP BY`：GroupBy 有支援的函式為 `MAX`、`MIN`、`SUM`、`AVG`
- `OFFSET | FETCH`
- `INNER JOIN`、`LEFT JOIN`：Join 最多可以到 7 個資料表

另外這邊不鼓勵 SELECT * 語法，所以一定要 SELECT 欄位，如果要 SELECT 所有欄位可以使用 `x => new { x }`，還有任何加入 SQL 運算的屬性都不可以被標記 `[NotMapped]`，範例程式碼如下，說明寫在註解中。

    private static async Task DemoQuery()
    {
        IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

        var memberDataAccess = dataAccessFactory.Create<Member>();

        // SELECT 一筆 Id = 1 的 Member
        var member = await memberDataAccess
                         .Where(x => x.Id == 1)
                         .Select(x => new { x.Name, x.Age })
                         .QueryOneAsync();

        // SELECT 多筆 Id IN (1, 2, 3) 的 Member
        var members = await memberDataAccess
                          .Where(x => new[] { 1, 2, 3 }.Contains(x.Id))
                          .Select(x => new { x.Name, x.Age })
                          .QueryAsync();

        // SELECT 出來的結果再轉成任意類別
        var partialMembers = await memberDataAccess
                                 .Where(x => new[] { 1, 2, 3 }.Contains(x.Id))
                                 .Select(x => new { x.Id, x.Name, x.Age })
                                 .QueryAsync(x => new { x.Id, x.Name });

        // SELECT 出來的結果再傳入 Aggregate 函式
        var totalAge = await memberDataAccess
                           .Where(x => new[] { 1, 2, 3 }.Contains(x.Id))
                           .Select(x => new { x.Id, x.Name, x.Age })
                           .QueryAsync(0, (accuAge, m) => accuAge + m.Age);

        // SELECT 多筆 Id NOT IN (1, 2, 3) 的 Member
        members = await memberDataAccess
                      .Where(x => !new[] { 1, 2, 3 }.Contains(x.Id))
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // SELECT TOP 10 筆 Age >= 30 AND Name LIKE 'J%' ORDER BY Age 的 Member
        members = await memberDataAccess
                      .Where(x => x.Age >= 30 && x.Name.StartsWith("J"))
                      .OrderBy(x => x.Age)
                      .Take(10)
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // SELECT (Id & 1) > 0 的 Member
        members = await memberDataAccess
                      .Where(x => (x.Id & 1) > 0)
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // SELECT 多筆 Age = 18 AND Name LIKE '%y' 的 Member，並傳回 Name, Age 欄位。
        members = await memberDataAccess
                      .Where(x => x.Age == 18 && x.Name.EndsWith("y"))
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // SELECT 多筆 Name LIKE '%e%' ORDER BY Id, Name DESC 的 Member
        members = await memberDataAccess
                      .Where(x => x.Name.Contains("e"))
                      .OrderBy(x => x.Id)
                      .ThenByDescending(x => x.Name)
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // 從全文檢索（Full-Text Search）SELECT 多筆 Name 含有 'Johnny' 的 Member
        members = await memberDataAccess
                      .Where(x => x.Name.Includes("Johnny"))
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // SELECT 多筆 Name NOT LIKE '%e%' ORDER BY Id, Name DESC 的 Member
        members = await memberDataAccess
                      .Where(x => !x.Name.Contains("e"))
                      .OrderBy(x => x.Id)
                      .ThenByDescending(x => x.Name)
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // 計算 Age = 20 的 Member 數量
        var memberCount = await memberDataAccess
                              .Where(x => x.Age == 20)
                              .CountAsync();

        // 計算 Age = 20 的 Member 數量（使用 Equals()）
        memberCount = await memberDataAccess
                          .Where(x => x.Age.Equals(20))
                          .CountAsync();

        // 計算 Age <> 20 的 Member 數量（使用 Equals()）
        memberCount = await memberDataAccess
                          .Where(x => !x.Age.Equals(20))
                          .CountAsync();

        // 回傳 Id = 1 的 Member 是否存在？
        var memberExists = await memberDataAccess
                               .Where(x => x.Id == 1)
                               .ExistsAsync();

        // 回傳 Name 有 "業務" 字樣的 Department 是否有 Member 存在？
        memberExists = await memberDataAccess
                           .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                           .Where((x, y) => y.Name.Contains("業務"))
                           .ExistsAsync();

        // 使用 And() 方法 SELECT 多筆 Age = 18 AND Name LIKE '%y' 的 Member，並傳回 Name, Age 欄位。
        members = await memberDataAccess
                      .Where(x => x.Age == 18)
                      .And(x => x.Name.EndsWith("y"))
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // 使用 Or() 方法 SELECT 多筆 Id IN (1, 2, 3) 的 Member
        members = await memberDataAccess
                      .Where(x => x.Id == 1)
                      .Or(x => x.Id == 2)
                      .Or(x => x.Id == 3)
                      .Select(x => new { x.Name, x.Age })
                      .QueryAsync();

        // 使用 Distinct() 方法 SELECT 不重複的 Member 名字
        members = await memberDataAccess
                      .Where(x => x.Id > 0)
                      .Distinct(x => new { x.Name })
                      .QueryAsync();

        // 使用 Skip() + Take() 方法跳過 20 筆取 10 筆資料，可以用做分頁查詢。
        members = await memberDataAccess
                      .OrderBy(x => x.Id)
                      .Select(x => new { x.Name, x.Age })
                      .Skip(20)
                      .Take(10)
                      .QueryAsync();

        // 使用 InnerJoin() 找出年紀大於 20 歲的 Member，以及 Member 的 Department 和 Manager。
        members = await memberDataAccess
                      .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                      .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                      .Where((x, y, z) => x.Age > 20)
                      .Select(
                          (x, y, z) => new
                                       {
                                           x.Id,
                                           x.Name,
                                           DepartmentId = y.Id,
                                           DepartmentName = y.Name,
                                           ManagerId = z.Id,
                                           ManagerName = z.Name
                                       })
                      .QueryAsync();

        var memberStatisticsDataAccess = dataAccessFactory.Create<MemberStatistics>();

        // 使用 GroupBy 找出 Member 名字叫 Johnny 的平均年齡
        var statistics = await memberStatisticsDataAccess
                             .Where(x => x.Name == "Johnny")
                             .GroupBy(
                                 x => new { x.Name },
                                 g => new MemberStatistics
                                      {
                                          Name = g.Select(x => x.Name),
                                          AverageAge = g.Avg(x => x.Age)
                                      })
                             .QueryOneAsync();
    }

### 一對多關係（One-To-Many）

如果要撈出`一對多關係（One-To-Many）`的結果，需要在資料類別以`主索引鍵`為參數，覆寫兩個方法：`Equals(object obj)` 及 `GetHashCode()`，主要是因為從資料庫回傳的原始結果是一個純二維的資料，在做關係對應的時候需要刪除重複資料（Deduplication），而且多做一件事情，就是當沒有主索引鍵的時候，必須呼叫各自的基礎方法。

假設 Member 及 Department 為一對多關係，Member 的主索引鍵為 Id 跟 Name，Department 的主索引鍵為 Id，調整後程式碼如下：

    [Table("tbl_member")]
    public class Member
    {
        [Column("No")]
        [Required]
        public int Id { get; set; }

        [StringLength(50)]
        [Required]
        public string Name { get; set; }

        // ...
        
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            if (this.Id == default) return base.Equals(obj);
            if (this.Name == default) return base.Equals(obj);

            return this.Equals((MemberForBenchmark)obj);
        }

        public override int GetHashCode()
        {
            if (this.Id == default) return base.GetHashCode();
            if (this.Name == default) return base.GetHashCode();

            return HashCode.Combine(this.Id, this.Name);
        }

        protected bool Equals(MemberForBenchmark other)
        {
            return this.Id == other.Id && this.Name == other.Name;
        }
    }

    [Table("tbl_department")]
    public class Department
    {
        [Required]
        public int Id { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            if (this.Id == default) return base.Equals(obj);

            return this.Equals((DepartmentForBenchmark)obj);
        }

        public override int GetHashCode()
        {
            if (this.Id == default) return base.GetHashCode();

            return this.Id;
        }

        protected bool Equals(DepartmentForBenchmark other)
        {
            return this.Id == other.Id;
        }
    }

而 Join 的語法就跟前面一樣，只是因為是一對多的關係，所以關聯的屬性改指定 `List<T>` 型別的集合。

    private static async Task DemoOneToManyQuery()
    {
        IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

        var memberDataAccess = dataAccessFactory.Create<Member>();

        // 使用 InnerJoin() 找出年紀大於 20 歲的 Member，以及 Member 的 Manager 和目前所掌管的所有 ManagedDepartments。
        var members = await memberDataAccess.InnerJoin(x => x.ManagedDepartments, (x, y) => x.Id == y.ManagerId)
                          .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                          .Where((x, y, z) => x.Age > 20)
                          .Select(
                              (x, y, z) => new
                                           {
                                               x.Id,
                                               x.Name,
                                               DepartmentId = y.Id,
                                               DepartmentName = y.Name,
                                               ManagerId = z.Id,
                                               ManagerName = z.Name
                                           })
                          .QueryAsync();
    }

※
運算子右邊的運算式僅支援`常數`、`變數`、`屬性`，除此之外的運算式是不支援的，這點需要特別注意。
