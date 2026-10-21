[創意料理] 單純對資料表簡單的 CRUD 讓 Chef.DbAccess.SqlServer 來協助我們 - Delete

## D - Delete

關於刪除資料的方法目前只有 7 個如下：

    public interface IDataAccess<T>
    {
        ...
        Task<int> DeleteAsync(Expression<Func<T, bool>> predicate);
        Task<int> DeleteAsync<TSecond>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, Expression<Func<T, TSecond, bool>> predicate);
        Task<int> DeleteAsync<TSecond, TThird>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, Expression<Func<T, TSecond, TThird, bool>> predicate);
        Task<int> DeleteAsync<TSecond, TThird, TFourth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate);
        Task<int> DeleteAsync<TSecond, TThird, TFourth, TFifth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate);
        Task<int> DeleteAsync<TSecond, TThird, TFourth, TFifth, TSixth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate);
        Task<int> DeleteAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate);
        ...
    }

範例程式碼如下，說明寫在註解中。

    private static async Task DemoDelete()
    {
        IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

        var memberDataAccess = dataAccessFactory.Create<Member>();

        // DELETE Name LIKE '%A%' 的 Member
        await memberDataAccess
            .Where(x => x.Name.Contains("A"))
            .DeleteAsync();

        // 刪除 Name 有 'A' 且 Department Name 有 '業務' 字樣的 Member
        await memberDataAccess
            .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
            .Where((x, y) => x.Name.Contains("A") && y.Name.Contains("業務"))
            .DeleteAsync();

        // DELETE Name LIKE '%A%' 的 Member，並且回傳已刪除的 Member 資料。
        var deletedMembers = await memberDataAccess
                                 .Where(x => x.Name.Contains("A"))
                                 .DeleteAsync(m => new { m.Id, m.Name });
    }
