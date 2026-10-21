[創意料理] 單純對資料表簡單的 CRUD 讓 Chef.DbAccess.SqlServer 來協助我們 - Create

## C - Create

關於新增資料的方法有 8 個如下：

    public interface IDataAccess<T>
    {
        ...
        Task<List<T>> InsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null);
        Task<int> InsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null);
        Task<List<T>> InsertAsync(IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null);
        Task<int> InsertAsync(IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null);
        Task<int> InsertAsync(T value, Expression<Func<T, bool>> nonexistence = null);
        Task<T> InsertAsync(T value, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null);
        Task<int> InsertAsync(Expression<Func<T>> setter, Expression<Func<T, bool>> nonexistence = null);
        Task<T> InsertAsync(Expression<Func<T>> setter, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null);
        ...
    }

其中如果要使用 `InsertAsync(T value)` 及 `InsertAsync(IEnumerable<T> values)` 這兩個方法的話，需要到對應的類別裡面，為指定的必填欄位標上 `[Required]`。

![](https://i.imgur.com/kaRYhen.png)

有 `output` 參數的 InsertAsync() 方法，可以讓我們回傳指定的新增欄位，這個用在 IDENTITY 識別屬性的欄位特別好用。

使用上建議優先選擇 [Chef.DbAccess.Fluent.Extention](https://github.com/supershowwei/Chef.DbAccess/blob/master/src/Chef.DbAccess/Chef.DbAccess/Fluent/Extension.cs) 提供的擴充方法，範例程式碼如下，說明寫在註解中。

    private static async Task DemoInsert()
    {
        var member = new Member { Id = 99, Name = "Kevin", Phone = "0000-000000", Age = 88 };

        var members = new List<Member>
                      {
                          new Member { Id = 98, Name = "Bob", Phone = "0000-000001", Age = 38 },
                          new Member { Id = 97, Name = "Tom", Phone = "0000-000002", Age = 85 }
                      };

        IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

        var memberDataAccess = dataAccessFactory.Create<Member>();

        // INSERT 一筆 Member，僅預設的必填欄位。
        await memberDataAccess.InsertAsync(member);

        // INSERT 多筆 Member，僅預設的必填欄位。
        await memberDataAccess.InsertAsync(members);

        // INSERT 一筆 Member，使用 Fluent 語法，僅 Id, Name, Phone 欄位。
        await memberDataAccess
            .Set(() => new Member { Id = 100, Name = "Pass", Phone = "0978-878787" })
            .InsertAsync();

        // INSERT 一筆 Member，使用 Fluent 語法，僅 Id, Name, Phone 欄位，回傳 Id 欄位。
        var outputMember = await memberDataAccess
                               .Set(() => new Member { Id = 100, Name = "Pass", Phone = "0978-878787" })
                               .InsertAsync(x => new { x.Id });

        // 上述語法也可以寫成下面這樣
        await memberDataAccess
            .Set(m => m.Id, 100)
            .Set(m => m.Name, "Pass")
            .Set(m => m.Phone, "0978-878787")
            .InsertAsync();

        // INSERT 多筆 Member，使用 Fluent 語法，僅 Id, Name, Age 欄位。
        await memberDataAccess.Set(() => new Member { Id = default(int), Name = default(string), Age = default(int) })
            .InsertAsync(
                new List<Member>
                {
                    new Member { Id = 11, Name = "Joe", Phone = "0000-000003", Age = 23 },
                    new Member { Id = 22, Name = "Steve", Phone = "0000-000004", Age = 45 }
                });

        // 上述語法也可以寫成下面這樣
        await memberDataAccess.Set(m => m.Id, default(int))
            .Set(m => m.Name, default(string))
            .Set(m => m.Age, default(int))
            .InsertAsync(
                new List<Member>
                {
                    new Member { Id = 11, Name = "Joe", Phone = "0000-000003", Age = 23 },
                    new Member { Id = 22, Name = "Steve", Phone = "0000-000004", Age = 45 }
                });
                
        // 當 nonexistence 參數的條件不存在時，才 INSERT 資料。
        var result = await memberDataAccess
            .Set(() => new Member { Id = 11, Name = "Joe", Phone = "0000-000003", Age = 23 })
            .InsertAsync(x => x.Id == 11);
    }
