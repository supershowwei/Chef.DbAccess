[創意料理] 單純對資料表簡單的 CRUD 讓 Chef.DbAccess.SqlServer 來協助我們 - Upsert

## U - Upsert

`Upsert` 是 UpdateOrInsert 的縮寫，顧名思義，它們作用就如同它的名稱，方法有 4 個如下：

    public interface IDataAccess<T>
    {
        ...
        Task<int> UpsertAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter);
        Task<T> UpsertAsync(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter, Expression<Func<T, object>> output);
        Task<int> UpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values);
        Task<List<T>> UpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output);
        ...
    }

它也有類似於 InsertAsync() 的 `output` 參數，一樣會回傳指定的資料欄位，範例程式碼如下，說明寫在註解中。

    private static async Task DemoUpsert()
    {
        IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

        var memberDataAccess = dataAccessFactory.Create<Member>();

        // UPDATE or INSERT 一筆 Id = 1 的 Member，包含 Name, Phone 欄位
        await memberDataAccess
            .Set(() => new Member { Name = "Black", Phone = "02-56567788" })
            .Where(x => x.Id == 1).UpsertAsync();

        // UPDATE or INSERT 一筆 Id = 1 的 Member，包含 Name, Phone 欄位，回傳 Id 欄位。
        var outputMember = await memberDataAccess
                               .Set(() => new Member { Name = "Black", Phone = "02-56567788" })
                               .Where(x => x.Id == 1)
                               .UpsertAsync(x => new { x.Id });

        // 上述語法也可以寫成下面這樣
        await memberDataAccess
            .Set(m => m.Name, "Black")
            .Set(m => m.Phone, "02-56567788")
            .Where(x => x.Id == 1).UpsertAsync();

        // UPDATE or INSERT 多筆 Member，包含 Age, Phone 欄位
        await memberDataAccess.Set(() => new Member { Name = default(string), Age = default(int), Phone = default(string) })
            .Where(x => x.Id == default(int))
            .UpdateAsync(
                new List<Member>
                {
                    new Member { Id = 77, Phone = "0000-000066", Age = 36 },
                    new Member { Id = 88, Phone = "0000-000077", Age = 57 }
                });

        // 上述語法也可以寫成下面這樣
        await memberDataAccess.Set(m => m.Name, default(string))
            .Set(m => m.Age, default(int))
            .Set(m => m.Phone, default(string))
            .Where(x => x.Id == default(int))
            .UpdateAsync(
                new List<Member>
                {
                    new Member { Id = 77, Phone = "0000-000066", Age = 36 },
                    new Member { Id = 88, Phone = "0000-000077", Age = 57 }
                });
    }
