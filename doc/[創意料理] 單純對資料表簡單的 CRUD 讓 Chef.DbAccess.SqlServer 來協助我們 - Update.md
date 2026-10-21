[創意料理] 單純對資料表簡單的 CRUD 讓 Chef.DbAccess.SqlServer 來協助我們 - Update

## U - Update

關於更新資料的方法有 8 個如下：

    public interface IDataAccess<T>
    {
        ...
        Task<int> UpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter);
        Task<int> UpdateAsync<TSecond>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, Expression<Func<T, TSecond, bool>> predicate, Expression<Func<T>> setter);
        Task<int> UpdateAsync<TSecond, TThird>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, Expression<Func<T, TSecond, TThird, bool>> predicate, Expression<Func<T>> setter);
        Task<int> UpdateAsync<TSecond, TThird, TFourth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate, Expression<Func<T>> setter);
        Task<int> UpdateAsync<TSecond, TThird, TFourth, TFifth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate, Expression<Func<T>> setter);
        Task<int> UpdateAsync<TSecond, TThird, TFourth, TFifth, TSixth>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate, Expression<Func<T>> setter);
        Task<int> UpdateAsync<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin, (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin, (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate, Expression<Func<T>> setter);
        Task<int> UpdateAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values);
        ...
    }

範例程式碼如下，說明寫在註解中。

    private static async Task DemoUpdate()
    {
        IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

        var memberDataAccess = dataAccessFactory.Create<Member>();

        // UPDATE 一筆 Id = 1 的 Member Phone 欄位
        await memberDataAccess
            .Set(() => new Member { Phone = "02-87871234" })
            .Where(x => x.Id == 1)
            .UpdateAsync();

        // 動態 UPDATE 一筆 Id = 1 的 Member Age, Phone 欄位
        await memberDataAccess
            .Set(x => x.Age, 20)
            .Set(x => x.Phone, "02-87871234")
            .Where(x => x.Id == 1)
            .UpdateAsync();

        // 動態 UPDATE Department Name 有 "業務" 字樣的 Member Age, Phone 欄位
        await memberDataAccess
            .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
            .Set(x => x.Age, 20)
            .Set(x => x.Phone, "02-87871234")
            .Where((x, y) => y.Name.Contains("業務"))
            .UpdateAsync();

        // UPDATE 多筆 Member 的 Age, Phone 欄位
        await memberDataAccess
            .Set(() => new Member { Age = default(int), Phone = default(string) })
            .Where(x => x.Id == default(int))
            .UpdateAsync(new List<Member>
                         {
                             new Member { Id = 44, Phone = "0000-000006", Age = 63 },
                             new Member { Id = 55, Phone = "0000-000007", Age = 75 }
                         });

        // 上述語法也可以寫成下面這樣
        await memberDataAccess
            .Set(m => m.Age, default(int))
            .Set(m => m.Phone, default(string))
            .Where(x => x.Id == default(int))
            .UpdateAsync(new List<Member>
                         {
                             new Member { Id = 44, Phone = "0000-000006", Age = 63 },
                             new Member { Id = 55, Phone = "0000-000007", Age = 75 }
                         });
    }
