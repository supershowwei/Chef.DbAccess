[創意料理] 單純對資料表簡單的 CRUD 讓 Chef.DbAccess.SqlServer 來協助我們 - Bulk

## Bulk 系列

Bulk 系列是使用 [Table-Valued Parameter](https://docs.microsoft.com/zh-tw/sql/relational-databases/tables/use-table-valued-parameters-database-engine) 來向資料庫傳遞參數，套件會自動在 SQL Server 建立暫時性的 [User Defined Table Type](https://www.c-sharpcorner.com/article/user-defined-table-types-and-table-valued-parameters/)，此系列操作資料的方法有 7 個如下：

    public interface IDataAccess<T>
    {
        ...
        Task<int> BulkInsertAsync(IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null);
        Task<List<T>> BulkInsertAsync(IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null);
        Task<int> BulkInsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, bool>> nonexistence = null);
        Task<List<T>> BulkInsertAsync(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output, Expression<Func<T, bool>> nonexistence = null);
        Task<int> BulkUpdateAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values);
        Task<int> BulkUpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values);
        Task<List<T>> BulkUpsertAsync(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output);
        ...
    }

BulkInsertAsync() 及 BulkUpsertAsync() 也一樣 output 參數可以回傳指定的資料欄位，範例程式碼如下，說明寫在註解中。

    private static async Task DemoBulk()
    {
        IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

        var memberDataAccess = dataAccessFactory.Create<Member>();

        // 批次 INSERT，只有預設的必填欄位。
        await memberDataAccess.BulkInsertAsync(
            new List<Member>
            {
                new Member { Id = 345, Name = "Logi" },
                new Member { Id = 678, Name = "Alucky" }
            });

        // 批次 INSERT，新增 Id, Name, Phone, Age 欄位。
        await memberDataAccess
            .Set(() => new Member { Id = default(int), Name = default(string), Phone = default(string), Age = default(int) })
            .BulkInsertAsync(
                new List<Member>
                {
                    new Member { Id = 324, Name = "Cruzer", Phone = "0000-000123", Age = 12 },
                    new Member { Id = 537, Name = "Slice", Phone = "0000-000456", Age = 21 }
                });

        // 批次 UPDATE Name, Phone 欄位
        await memberDataAccess
            .Where(x => x.Id == default(int))
            .Set(() => new Member { Name = default(string), Phone = default(string), })
            .BulkUpdateAsync(
                new List<Member>
                {
                    new Member { Id = 324, Name = "Lawson", Phone = "0000-000523" },
                    new Member { Id = 537, Name = "Marlon", Phone = "0000-000756" }
                });

        // 批次 UPDATE or INSERT Name, Phone 欄位
        await memberDataAccess
            .Where(x => x.Id == default(int))
            .Set(() => new Member { Name = default(string), Phone = default(string), })
            .BulkUpsertAsync(
                new List<Member>
                {
                    new Member { Id = 324, Name = "Ramone", Phone = "0000-000623" },
                    new Member { Id = 657, Name = "Chris", Phone = "0000-000835" }
                });

        // 批次 UPDATE or INSERT Name, Phone 欄位，回傳 Id 欄位。
        await memberDataAccess
            .Where(x => x.Id == default(int))
            .Set(() => new Member { Name = default(string), Phone = default(string), })
            .BulkUpsertAsync(
                new List<Member>
                {
                    new Member { Id = 324, Name = "Ramone", Phone = "0000-000623" },
                    new Member { Id = 657, Name = "Chris", Phone = "0000-000835" }
                },
                x => new { x.Id });
    }
