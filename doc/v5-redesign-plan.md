# Chef.DbAccess 5.0 改版規劃：沒有 DbContext 的 LINQ Provider

> 狀態：草案（2026-11-01）；§12 的 D1～D15 已定案
> 分支：`v5/redesign`（由本機 `master` @ `044280a`，也就是 3.x 開出）
> 範圍：Chef.DbAccess（核心）、Chef.DbAccess.SqlServer（SQL Server 實作）、DI 擴充套件與測試專案
> 參考：`efcore/`（Entity Framework Core 原始碼，只作為設計參考，不引用、不複製）

---

## 0. 摘要

| 項目 | 內容 |
|------|------|
| 一句話 | 讓 `factory.Create<Member>()` 拿到的物件**本身就是 `IQueryable<Member>`**，用標準 LINQ 自由組合，由運算式樹動態產生 SQL；不需要 DbContext、不追蹤變更、寫入一律明確呼叫。 |
| 借鏡 EF Core 的部分 | 查詢管線的分層：Funcletizer → 快取 → 正規化 → 導覽展開 → `QueryableMethod` 翻譯 → SQL AST → Null 語意與參數後處理 → SQL 產生 → 編譯好的 Shaper。 |
| 捨棄 EF Core 的部分 | DbContext、Change Tracker、`SaveChanges`、Migration、Lazy Loading、DI 服務容器式的內部架構、Model 建置期的複雜慣例系統。 |
| 保留 3.x 的特色 | DataAnnotations 對應、以類別為主（`[Required]` 決定 Insert 欄位）、數值與布林值以字面值內嵌（`{=}`，避免參數探查）、`[ConnectionString]`、跨資料庫 Join、`varchar` 正確對應、全文檢索 `Includes()`、`OUTPUT`、Upsert、Bulk 系列、`OutputSql`／`OnDbError`、member-init 寫法的 setter。 |
| 破壞性 | 主版號升級，核心 API 重新設計；**3.x API 直接移除，不提供相容套件**，升級時改寫程式碼（§7 提供對照表）。不再相依 Dapper。 |
| 目標框架 | 只有 `netstandard2.0`：不會因為某一版 .NET 停止支援而必須改版（D6）。 |
| SQL Server 基準 | 2016 SP1，任何資料庫相容性層級；不使用 `OPENJSON`、`STRING_SPLIT` 與 2017 之後的語法（D14、§5.16）。 |
| 效能 | 移除 Dapper 但不慢於 Dapper：寫成每個 preview 的放行條件，Raw SQL 路徑保留委派回 Dapper 的退路（§10）。 |
| 階段 | Phase 0（工程整頓）→ 1（Metadata／型別對應）→ 2（SQL AST）→ 3（單表 IQueryable，preview1）→ 4（Join／導覽，preview2）→ 5（子查詢／GroupBy／集合運算，preview3）→ 6（寫入，preview4）→ 7（文件、範例與試升級，RC）→ 8（效能與正式版）。 |

---

## 1. 目標與設計原則

### 1.1 定位

Chef.DbAccess 5.0 是一個**輕量的 LINQ Provider**：

1. **類別即資料表**：一個加上 DataAnnotations 的 POCO 類別就是一張資料表的對應，`factory.Create<T>()` 立即可查可寫，不需要宣告 DbContext、不需要註冊 `DbSet<T>`、不需要 `OnModelCreating`。
2. **Lambda 自由組成 SQL**：`Where`、`Select`、`Join`、`GroupBy`、子查詢、集合運算等標準 LINQ 運算子都可以任意串接、拆開、重組，最後才一次翻譯成 SQL。
3. **輕**：沒有 Change Tracking、Identity Map、Lazy Loading、Migration。查出來的物件就是普通物件；寫入就是明確的 `InsertAsync`／`ExecuteUpdateAsync`／`UpsertAsync`……。

### 1.2 設計原則

| # | 原則 | 說明 |
|---|------|------|
| P1 | **不需要 DbContext** | 對應資訊來自類別本身（DataAnnotations＋慣例），必要時再用全域的 Fluent 設定補充；查詢根就是 `IDataAccess<T>`。 |
| P2 | **先組合、後翻譯** | 所有 LINQ 運算子只累積運算式樹，直到 `ToListAsync()`、`FirstAsync()`、`ExecuteUpdateAsync()` 等終結運算子才翻譯並執行。 |
| P3 | **一律經過 SQL AST** | 運算式樹先翻成與資料庫無關的 SQL AST，最後才由方言輸出字串；不再在字串上拼接或用 Regex 反推。 |
| P4 | **翻譯一次、重複使用** | 以「運算式形狀」作為快取鍵，值不進快取鍵；同一個形狀只翻譯、編譯一次。 |
| P4a | **避免參數探查** | 數值與布林值預設以字面值內嵌進 SQL（延續 3.x 的 `{=}`），讓 SQL Server 依實際值編譯執行計畫；字串、日期等其他型別才參數化（§5.7）。內嵌不破壞 P4：SQL 範本只產生一次，執行時只把值填進插槽。 |
| P5 | **翻不了就明說** | 除了最外層投影之外，絕不在用戶端偷偷計算；無法翻譯時丟出附帶運算式內容的 `ChefQueryTranslationException`（EF Core 3.0 之後的原則）。 |
| P6 | **C# 語意優先** | `x.Name == name` 在 `name` 為 `null` 時產生 `IS NULL`；兩個可為 null 的欄位比較時補上 null 判斷，讓結果與 LINQ to Objects 一致。 |
| P7 | **SQL Server 優先，方言可抽換** | 核心不相依 SqlClient；SQL Server 是第一個也是 5.0 唯一正式支援的方言，但架構要能接上 SQLite／PostgreSQL。 |
| P8 | **介面要小** | `IDataAccess<T>` 只放查詢根與必要設定；寫入、非同步運算子、提示（hint）等都做成擴充方法，新增功能不再破壞實作者。 |

### 1.3 明確不做的事（Non-goals）

DbContext、Change Tracking、`SaveChanges`／Unit of Work 自動排序、Identity Map（跨查詢）、Lazy Loading、Proxy、Migration、Scaffolding、Owned／Complex Type（5.0 不做）、多型繼承對應（TPH／TPT／TPC，5.0 不做）、Cosmos 等非關聯式資料庫。

---

## 2. 目標使用體驗（v5 API 預覽）

> 以下程式碼是 5.0 的目標 API，用來對齊方向；實作時細節可能調整，但精神不變。

### 2.1 定義模型

```csharp
[ConnectionString("MemberDB")]
[Table("tbl_member")]
public class Member
{
    [Key, Column("No")]
    public int Id { get; set; }

    [Required, StringLength(50)]
    public string Name { get; set; }

    [Column(TypeName = "varchar"), StringLength(20)]
    public string Phone { get; set; }                  // 參數會是 varchar(20)，不會造成隱含轉型

    public int Age { get; set; }

    public DateTime CreatedAt { get; set; }

    public int DepartmentId { get; set; }

    [ForeignKey(nameof(DepartmentId))]
    public Department Department { get; set; }         // 不可為 null 的外鍵 → INNER JOIN

    public int? ManagerId { get; set; }

    [ForeignKey(nameof(ManagerId))]
    public Member Manager { get; set; }                // 可為 null 的外鍵 → LEFT JOIN

    [InverseProperty(nameof(Department.Manager))]
    public List<Department> ManagedDepartments { get; set; }  // 一對多集合

    [NotMapped]
    public string Secret { get; set; }
}

[ConnectionString("MemberDB")]
[Table("tbl_department")]
public class Department
{
    [Key]
    public int Id { get; set; }

    [Required]
    public string Name { get; set; }

    public int? ManagerId { get; set; }

    [ForeignKey(nameof(ManagerId))]
    public Member Manager { get; set; }
}
```

### 2.2 建立查詢根

```csharp
var factory = SqlServerDataAccessFactory.Instance;
factory.AddConnectionString("MemberDB", configuration.GetConnectionString("MemberDB"));

IDataAccess<Member> members = factory.Create<Member>();         // 本身就是 IQueryable<Member>
IDataAccess<Department> departments = factory.Create<Department>();
```

### 2.3 查詢

```csharp
// 條件、排序、投影到 DTO、分頁
var page = await members
    .Where(x => x.Age >= 30 && x.Name.StartsWith(prefix) && x.CreatedAt > DateTime.Today.AddDays(-7))
    .OrderBy(x => x.Age).ThenByDescending(x => x.Id)
    .Select(x => new MemberDto { Id = x.Id, Name = x.Name, DepartmentName = x.Department.Name })
    .Skip((pageIndex - 1) * pageSize).Take(pageSize)
    .ToListAsync(cancellationToken);
```

```sql
-- pageIndex = 3、pageSize = 10 時
-- 數值（30、分頁數字）以字面值內嵌；字串 prefix 與用戶端算出的日期參數化
-- （@prefix_startswith 的值是已跳脫萬用字元的 'J%'；@p0 是 DateTime.Today.AddDays(-7) 在用戶端的結果）
SELECT [m].[No] AS [Id], [m].[Name], [d].[Name] AS [DepartmentName]
FROM [tbl_member] AS [m] WITH (NOLOCK)
INNER JOIN [tbl_department] AS [d] WITH (NOLOCK) ON [m].[DepartmentId] = [d].[Id]
WHERE [m].[Age] >= 30 AND [m].[Name] LIKE @prefix_startswith ESCAPE N'\' AND [m].[CreatedAt] > @p0
ORDER BY [m].[Age], [m].[No] DESC
OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;
```

> 別名預設取類別名稱的第一個字母（`Member` → `[m]`），重複時加流水號（`[m0]`）。
>
> 要在伺服器端求值時改寫成 `x.CreatedAt > ChefFunctions.ServerToday().AddDays(-7)`，產生 `DATEADD(day, -7, CONVERT(date, GETDATE()))`（D11）。

```csharp
// 只取部分欄位但仍然回傳 Member（取代 3.x 的 Select(x => new { x.Name, x.Age })）
var partial = await members.Where(x => ids.Contains(x.Id))
                           .Select(x => new Member { Id = x.Id, Name = x.Name })
                           .ToListAsync();

// 動態組條件：IQueryable 本來就可以分段組合
IQueryable<Member> query = members;
if (minAge.HasValue) query = query.Where(x => x.Age >= minAge);
if (!string.IsNullOrEmpty(keyword)) query = query.Where(x => x.Name.Contains(keyword));
var total = await query.CountAsync();
var rows = await query.OrderBy(x => x.Id).Take(50).ToListAsync();

// 單筆
var one = await members.SingleOrDefaultAsync(x => x.Id == id);
var exists = await members.AnyAsync(x => x.Phone == phone);
```

### 2.4 Join、導覽屬性與 Include

```csharp
// 1) 標準 LINQ Join（不需要任何關聯設定）
var q = from m in members
        join d in departments on m.DepartmentId equals d.Id
        where d.Name.Contains(keyword)
        select new { m.Name, DepartmentName = d.Name };

// 2) Left Join：GroupJoin + SelectMany + DefaultIfEmpty，或 LeftJoin
//    （.NET 10 以上直接用 Queryable.LeftJoin；更舊的執行環境 using Chef.DbAccess.Linq 取得 Chef 提供的同名方法）
var q2 = members.LeftJoin(departments, m => m.DepartmentId, d => d.Id, (m, d) => new { m.Name, Dept = d.Name });

// 3) 在 Lambda 內直接走導覽屬性 → 自動產生 JOIN
var q3 = members.Where(x => x.Department.Name == "業務部").Select(x => new { x.Name, ManagerName = x.Manager.Name });

// 4) 集合導覽屬性 → EXISTS
var q4 = members.Where(x => x.ManagedDepartments.Any(d => d.Name.Contains("業務")));

// 5) Include：把關聯資料填回導覽屬性（一對多以主鍵做 identity resolution）
var list = await members
    .Include(x => x.Department)
    .Include(x => x.ManagedDepartments)
    .Where(x => x.Age > 20)
    .ToListAsync();

// 6) 沒有標註關聯時，延續 3.x 的「直接給 Join 條件」寫法
var list2 = await members
    .Include(x => x.ManagedDepartments, (m, d) => m.Id == d.ManagerId)
    .ToListAsync();
```

### 2.5 GroupBy、子查詢、集合運算

```csharp
var stats = await members
    .GroupBy(x => x.DepartmentId)
    .Where(g => g.Count() > 5)                       // → HAVING
    .Select(g => new { DepartmentId = g.Key, Count = g.Count(), AvgAge = g.Average(x => x.Age) })
    .ToListAsync();

// 相關子查詢（departments 是閉包裡的 IQueryable，會被內嵌成子查詢而不是被求值）
var managers = members.Where(m => departments.Any(d => d.ManagerId == m.Id));

// 純量子查詢
var withCount = members.Select(m => new { m.Name, Managed = departments.Count(d => d.ManagerId == m.Id) });

// 集合運算
var union = members.Where(x => x.Age < 20).Select(x => x.Name)
                   .Union(members.Where(x => x.Age > 60).Select(x => x.Name));
```

### 2.6 寫入

```csharp
// INSERT：寫入標了 [Required] 的屬性（延續 3.x，D3）；類別可以只描述資料表的一部分
await members.InsertAsync(member);
var newId = await members.InsertAsync(member, returning: x => x.Id);              // OUTPUT INSERTED.[No]
await members.InsertAsync(() => new Member { Name = "Johnny", Phone = "0912" });  // 只寫入指定欄位（3.x setter 寫法）
await members.InsertAsync(memberList);                                            // 多列 VALUES，依參數上限自動分批
await members.InsertAsync(member, whenNotExists: x => x.Name == member.Name);     // INSERT ... WHERE NOT EXISTS

// set-based UPDATE／DELETE（不需要先查詢）
await members.Where(x => x.Id == id)
             .ExecuteUpdateAsync(x => new Member { Phone = phone, Age = x.Age + 1 });   // Chef 風格：member-init
await members.Where(x => x.Age < 18)
             .ExecuteUpdateAsync(s => s.SetProperty(x => x.Age, x => x.Age + 1));       // EF Core 風格
await members.Where(x => x.Department.Name.Contains("業務")).ExecuteDeleteAsync();      // 可以搭配 JOIN
var deleted = await members.Where(x => x.Age > 100)
                           .ExecuteDeleteAsync(returning: x => new { x.Id, x.Name });   // OUTPUT DELETED.*

// UPSERT
await members.UpsertAsync(member, on: x => x.Id);
await members.UpsertAsync(member, on: x => new { x.Name, x.Phone }, update: x => new { x.Age });

// BULK（SqlBulkCopy ＋ 連線範圍的 #暫存資料表，不建立任何資料庫物件，見 §5.12.3）
await members.BulkInsertAsync(bigList);                                     // [Required] 欄位
await members.BulkInsertAsync(bigList, columns: x => new { x.Name, x.Phone });  // 指定欄位
await members.BulkUpdateAsync(bigList, on: x => x.Id, columns: x => new { x.Name, x.Phone });
var outputs = await members.BulkUpsertAsync(bigList, on: x => x.Id, returning: x => new { x.Id });
```

### 2.7 Raw SQL、交易、診斷

```csharp
// 可組合的 Raw SQL（插值套用 §5.7 規則：age 是 int 所以內嵌，字串則參數化）
var adults = await members.FromSql($"SELECT * FROM tbl_member WHERE Age >= {age}")
                          .Where(x => x.Name.StartsWith("J"))
                          .ToListAsync();

// 任意型別的 Raw SQL：Chef 自己的實作（不再透過 Dapper），寫法與 3.x 相同
//   @name → 參數、{=name} → 字面值內嵌、IN @list → 展開
var rows = await members.QueryAsync<ReportRow>(
    "SELECT * FROM tbl_member WHERE Age >= {=MinAge} AND Name = @Name AND DepartmentId IN @DeptIds",
    new { MinAge = 30, Name = name, DeptIds = new[] { 1, 2, 3 } });
await members.ExecuteAsync("EXEC dbo.usp_Report @Year", new { Year = 2026 });

// 交易：同一個連線字串的操作自動加入（AsyncLocal）
await using (var tx = await factory.BeginTransactionAsync("MemberDB"))
{
    await members.InsertAsync(member);
    await departments.Where(d => d.Id == member.DepartmentId).ExecuteUpdateAsync(d => new Department { Name = d.Name });
    await tx.CommitAsync();
}
// System.Transactions.TransactionScope（TransactionScopeAsyncFlowOption.Enabled）同樣支援

// 診斷
string sql = members.Where(x => x.Id == id).ToQueryString();   // 含 DECLARE 參數宣告，可以直接貼到 SSMS 執行
members.OutputSql = (sql, parameters) => logger.LogDebug(sql);   // 3.x 掛鉤保留，而且一律帶參數

// 查詢層級的提示
var fresh = await members.WithoutNoLock().TagWith("報表：月結").WithCommandTimeout(120).ToListAsync();

// 預先編譯的查詢（省掉快取查找）
static readonly Func<IDataAccess<Member>, int, CancellationToken, Task<Member>> MemberById =
    ChefQuery.CompileAsync((IDataAccess<Member> m, int id) => m.SingleOrDefault(x => x.Id == id));
```

### 2.8 相依性注入（選用）

```csharp
services.AddChefDbAccess(o => o
    .UseSqlServer()
    .AddConnectionString("MemberDB", configuration.GetConnectionString("MemberDB"))
    .UseLoggerFactory(loggerFactory));

public class MemberService(IDataAccess<Member> members) { ... }   // 開放泛型註冊
```

---

## 3. 現況盤點（3.x）

### 3.1 值得保留的優點

- **零設定**：DataAnnotations（`[Table]`、`[Column]`、`[Required]`、`[StringLength]`、`[NotMapped]`）就能對應；`[ConnectionString]`＋`AddConnectionString` 的連線解析簡單好用。
- **以類別為主，不被資料表綁死**：`[Required]` 決定 `InsertAsync(entity)` 要寫入哪些欄位，一個類別可以只描述資料表的一部分，同一張表也可以有多個用途不同的類別。EF Core 的模型必須和資料表完整對齊，這是 Chef 刻意不同的地方，5.0 保留（D3）。
- **避免參數探查**：數值與布林值以 Dapper 的 `{=name}` 字面值替換內嵌進 SQL（`StatementExtension.NumericTypes`、`GenerateParameterStatement`，`StatementExtension.cs:20`、`:1641`），條件、`SET`、`VALUES` 都一樣。SQL Server 因此依實際值編譯執行計畫，不會因為第一次的參數值而沿用不適合的計畫。5.0 保留（D5），改由 Chef 自己在 SQL 產生階段處理。
- **SQL Server 實務細節**：`[Column(TypeName = "varchar")]` 會轉成 `DbString`（避免 `nvarchar` 參數造成索引失效）、`OUTPUT`、Upsert、Bulk（用完即刪的 TVP 型別，不留下資料庫物件）、全文檢索 `Includes()`→`CONTAINS`、跨資料庫 Join（`[Database].[Schema].[Table]`）。
- **member-init 的 setter 寫法**：`Set(() => new Member { Phone = "..." })` 很直覺，5.0 要延續到 `ExecuteUpdateAsync` 與 `InsertAsync`。
- **Join 結果填回導覽屬性**（含一對多），這是 Dapper 沒有、使用者很常用的能力。
- `OutputSql`、`OnDbError` 兩個掛鉤。

### 3.2 架構瓶頸

| # | 問題 | 位置 | 影響 |
|---|------|------|------|
| A1 | **泛型參數數量爆炸**：每個操作為 1～7 張表各寫一份 | `IDataAccess.cs`（472 行）、`QueryObject.cs`、`Fluent/Extension.cs`（1,753 行）、`SqlServerDataAccess.cs`（4,697 行）、`StatementExtension.cs`（1,689 行） | 最多只能 Join 7 張表；改一個功能要改 7 份；介面肥大，任何新增都是破壞性變更 |
| A2 | `QueryObject` 是可變的屬性袋，不能組合 | `QueryObject.cs` | 沒有子查詢、Union、巢狀投影；同一個物件被就地修改，無法安全地分支重用 |
| A3 | 直接在 `StringBuilder` 上拼 SQL，沒有中介模型 | `StatementExtension.ParseCondition`（`StatementExtension.cs:1093`） | 很難支援函式、算術、子查詢，也無法換資料庫 |
| A4 | 用 Regex 從**產生出來的 SQL 字串**反推欄位清單 | `ColumnValueRegex`（`SqlServerDataAccess.cs:23`）、`ResolveColumnList`（`:1983`），用在 Upsert／BulkUpsert | 遇到函式、子查詢、特殊名稱就會壞 |
| A5 | 每次執行都重新解析運算式，沒有快取 | 所有 `Generate*Statement` | 反射與字串組裝成本每次都付 |
| A6 | `Select` 的型別是 `Expression<Func<T, object>>`，只能投影回 `T` | `IDataAccess.cs` | 不能直接投影成 DTO、匿名型別或純量 |
| A7 | 核心與 SQL Server 綁死 | — | 無法支援其他資料庫，也無法用 SQLite 做快速測試 |
| A8 | `NOLOCK` 是先寫進字串再用 `Replace(" WITH (NOLOCK)", "")` 拿掉 | `SqlServerDataAccess.cs:2083` 等 7 處 | 脆弱；無法針對單一查詢或單一資料表調整 |
| A9 | 一對多合併依賴 `Dictionary<T, T>`，也就是模型自己覆寫的 `Equals`／`GetHashCode` | `SqlServerDataAccess.cs:86` 起的 `ExecuteQuery*Async` | 沒覆寫就無法正確合併（範例的 `*ForBenchmark` 類別就是為此手寫 `Equals`） |

### 3.3 運算式翻譯的限制（使用者會直接碰到）

- 左邊一定要是欄位：`5 < x.Age`、`x.Age + 1 > 5` 會丟 `Left expression must be MemberExpression.`（`StatementExtension.cs:1164`）；`x.Department.Name == ...` 這種非直接參數的成員會丟 `Parameter expression must be placed left.`（`:1170`）。
- 右邊只能是常數、欄位或屬性：`x.CreatedAt > DateTime.Now.AddDays(-1)`、`x.Id == GetId()` 會丟例外（`:1488`）。
- 不支援 `x.Name.Length`、`ToUpper()`、`Trim()`、`string.IsNullOrEmpty`、`x.CreatedAt.Year`、`Math.*`、`?:`、`??`、`HasValue`、算術運算。
- `ids.Contains(x.Id)` 展開成 `OR` 串（`:1360` 起），資料多時容易超過 SQL Server 2,100 個參數的上限；`new int[3].Contains(...)` 這種指定長度的陣列會把長度當成元素（`ExtractArray`，`:1507`）。
- 字面值內嵌依賴 Dapper 的 `{=name}`，所以只能用在經過 Dapper 的路徑；字串拼接時也無法依欄位型別決定格式（例如 `float` 的精確度）。
- `LIKE` 的值沒有跳脫 `%`、`_`、`[`。

### 3.4 API 陷阱

- `QueryObject.Where()` 會**覆蓋**先前的條件（`Fluent/Extension.cs:111`），連續兩次 `.Where()` 時第一個條件被悄悄丟掉。
- 沒先 `Where` 就 `And`／`Or`、沒先 `OrderBy` 就 `ThenBy`，會發生 `NullReferenceException`（`:118`、`:168`）。
- 沒指定 `Select` 時直接丟例外（`SqlServerDataAccess.cs:2027`），不會預設選出全部欄位。
- `GroupBy` 必須搭配假的 `Grouping<T>` 類別（方法本體全是 `NotImplementedException`），學習成本高且無法和其他運算子組合。
- 全部 API 都沒有 `CancellationToken`。
- Update、Delete、Upsert、Bulk、Insert(entity) 呼叫 `OutputSql` 時不帶參數（`SqlServerDataAccess.cs:3389`、`:3867` 等 10 處以上）。

### 3.5 寫入面

- Bulk 系列每次執行都 `CREATE TYPE`／`DROP TYPE`（`SqlServerDataAccess.cs:3956`、`:3962` 等）。目的是用完即刪、不留下資料庫物件，但有幾個代價：需要使用者資料庫的 `CREATE TYPE` 權限；每次都寫入使用者資料庫的系統目錄；`CREATE` 與 `DROP` 之間程序中斷（逾時、當機、部署重啟）時型別會**殘留**；一次 Bulk 要多兩趟 DDL 往返。§5.12.3 改用連線範圍的 `#暫存資料表`。
- Upsert 用 `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` 改變整個 session 的隔離等級，影響範圍超過該敘述本身；應改用只作用於該敘述的 table hint。
- `OUTPUT` 一律走「`SELECT ... INTO #tmp WHERE 1 = 0` → `OUTPUT INTO #tmp` → `SELECT` → `DROP`」四段式，即使資料表沒有觸發程序也一樣。
- 為了繞過 Dapper 查詢快取的問題，遇到 SqlException 137／102 時清空快取重試（`IDbConnectionExtension.cs:26`）。

### 3.6 工程面

- `Chef.DbAccess.SqlServer` 透過 **NuGet 3.0.0** 參考 `Chef.DbAccess`，不是 ProjectReference，無法同步開發。
- 測試專案是 .NET Framework 4.6.2 的舊式 csproj＋`packages.config`（MSTest），而且幾乎全是需要 LocalDB 與實體資料庫（`test/DB/*.mdf`）的整合測試，沒有不需要資料庫的 SQL 快照測試。
- 範例專案是 `netcoreapp3.1`，參考 NuGet 3.0.3。
- 沒有 CI，也沒有效能基準。

---

## 4. 從 EF Core 借鏡什麼、捨棄什麼

### 4.1 EF Core 查詢管線概覽

```
IQueryable.Expression
  │ EntityQueryProvider.Execute*                       efcore/src/EFCore/Query/Internal/EntityQueryProvider.cs
  ▼
QueryCompiler.ExecuteAsync                             efcore/src/EFCore/Query/Internal/QueryCompiler.cs
  ├─ ExtractParameters（ExpressionTreeFuncletizer）     efcore/src/EFCore/Query/Internal/ExpressionTreeFuncletizer.cs
  ├─ CompiledQueryCacheKeyGenerator（ExpressionEqualityComparer）
  │                                                     efcore/src/EFCore/Query/CompiledQueryCacheKeyGenerator.cs
  │                                                     efcore/src/EFCore/Query/ExpressionEqualityComparer.cs
  └─ CompileQueryCore（快取沒命中才做）
       ├─ QueryTranslationPreprocessor                  efcore/src/EFCore/Query/QueryTranslationPreprocessor.cs
       │    ├─ QueryableMethodNormalizingExpressionVisitor   …/Query/Internal/QueryableMethodNormalizingExpressionVisitor.cs
       │    └─ NavigationExpandingExpressionVisitor           …/Query/Internal/NavigationExpandingExpressionVisitor*.cs
       ├─ RelationalQueryableMethodTranslatingExpressionVisitor → ShapedQueryExpression(SelectExpression, Shaper)
       │                                                efcore/src/EFCore.Relational/Query/RelationalQueryableMethodTranslatingExpressionVisitor*.cs
       │    └─ RelationalSqlTranslatingExpressionVisitor（Lambda 本體 → SqlExpression）
       │         └─ Member／MethodCall／Aggregate Translator Provider（方言擴充點）
       ├─ QueryTranslationPostprocessor（SqlTreePruner…）
       └─ RelationalShapedQueryCompilingExpressionVisitor（編譯 Shaper → QueryingEnumerable）
            └─ RelationalCommandCache：依參數是否為 null、集合長度，執行期再跑
                 SqlNullabilityProcessor ＋ RelationalParameterBasedSqlProcessor → QuerySqlGenerator
```

### 4.2 元件對照：採用程度

| EF Core 元件 | Chef v5 對應 | 採用程度 | 說明 |
|---|---|---|---|
| `EntityQueryable<T>`、`EntityQueryProvider`、`IAsyncQueryProvider` | `SqlServerDataAccess<T>`（實作 `IOrderedQueryable<T>`）、`ChefQueryProvider` | 採用 | 查詢根就是 data access 本身，等同 `DbSet<T>` 的角色 |
| `EntityQueryRootExpression` | `DataAccessQueryRootExpression` | 採用 | 帶著 `EntityType` 與 `DataSource`（伺服器、資料庫、資料表、hint） |
| `ExpressionTreeFuncletizer` | `ParameterExtractor` | 採用（簡化） | 閉包變數 → 參數；可求值子樹 → 參數；閉包中的 `IQueryable` → 內嵌其運算式 |
| `ExpressionEqualityComparer`、`CompiledQueryCacheKeyGenerator` | `ExpressionShapeComparer`、`QueryCacheKey` | 採用 | 快取鍵再加上方言、`DataSource` 身分、hint、async 旗標 |
| `QueryableMethodNormalizingExpressionVisitor` | `QueryNormalizer` | 採用（簡化） | GroupJoin＋SelectMany＋DefaultIfEmpty → LeftJoin、查詢語法的透明識別項、`Contains`／`Count` 等形式統一 |
| `NavigationExpandingExpressionVisitor` | `NavigationExpander` | 部分採用 | 只處理參考導覽（JOIN）、集合導覽在條件中（EXISTS）、`Include`；不做 owned、不做全域篩選 |
| `RelationalQueryableMethodTranslatingExpressionVisitor` | `QueryableMethodTranslator` | 採用 | 運算子 → `SelectExpression` 操作；包含 pushdown 判斷 |
| `RelationalSqlTranslatingExpressionVisitor` | `SqlTranslator` | 採用 | Lambda 本體 → `SqlExpression` |
| `SqlExpressionFactory`、TypeMapping 推導 | `SqlExpressionFactory`、`TypeMappingSource` | 採用（簡化） | 每個 SQL 節點帶型別對應，參數型別由欄位推導（例如 varchar 欄位 → varchar 參數） |
| `SqlExpressions/*`（`SelectExpression`、`ColumnExpression`…） | `Chef.DbAccess.Query.SqlExpressions` | 採用子集 | 見 §5.4.4；不做 JSON、Vector、Temporal、Window（5.x 再評估） |
| `IMemberTranslator`／`IMethodCallTranslator`／`IAggregateMethodCallTranslator` ＋ Plugin | 同名介面 ＋ `TranslatorRegistry` | 採用 | 使用者可以註冊自己的翻譯器 |
| `SqlNullabilityProcessor` | `NullSemanticsProcessor` | 採用（簡化） | C# null 語意補償；可選擇關閉（`UseRelationalNulls`） |
| `RelationalParameterBasedSqlProcessor`、`RelationalCommandCache`、`ParameterTranslationMode` | `ParameterBasedSqlProcessor`、`CommandCache` | 採用 | 依參數 null 與否、集合長度產生最終 SQL 並快取 |
| `QuerySqlGenerator`、`SqlServerQuerySqlGenerator` | `QuerySqlGenerator`、`SqlServerQuerySqlGenerator` | 採用 | 另外支援 table hint、`OPTION(...)`、跨資料庫三段式名稱 |
| `StructuralTypeMaterializerSource`、Shaper 編譯 | `ShaperCompiler` | 採用（簡化） | 直接產生 `Func<DbDataReader, ..., T>`；不經過 Change Tracker |
| `SingleQueryCollectionContext`、identity resolution | `CollectionShaper` | 採用 | 以主鍵合併一對多 |
| `SplitQueryCollectionContext`（`AsSplitQuery`） | — | 5.x | 5.0 先只做單一查詢 |
| `ExecuteUpdate`／`ExecuteDelete`（`UpdateExpression`、`DeleteExpression`、`UpdateSettersBuilder`） | 同名概念 | 採用 | 另外支援 Chef 的 member-init setter 與 `OUTPUT` |
| `FromSqlExpression`、`SqlQuery<T>` | `FromSql`、`QueryAsync<T>` | 採用 | |
| `ExpressionPrinter` | `ExpressionPrinter`（自寫簡版） | 採用 | 例外訊息與除錯 |
| `QueryTestBase`／`QueryAsserter`（同一查詢對資料庫與記憶體資料各跑一次比對） | `QueryAsserter` | 採用 | 測試策略核心，見 §9 |
| DbContext、`IServiceProvider` 內部容器、Change Tracker、Model 建置、Migration、`LiftableConstant`／Precompiled Query | — | 捨棄 | 不符合輕量定位；precompiled query 5.x 再評估 |

### 4.3 刻意與 EF Core 不同的地方

| 主題 | EF Core | Chef v5 | 理由 |
|---|---|---|---|
| 查詢根 | `context.Set<T>()` | `factory.Create<T>()`，可以在任何地方建立 | 不需要 DbContext |
| 模型 | 啟動時一次建好整個 Model | **每個型別第一次用到時才解析並快取**（`EntityTypeCache<T>`） | 不需要事先宣告所有實體 |
| 關聯 | 慣例＋Fluent 建立完整關聯圖 | `[ForeignKey]`／`[InverseProperty]`／命名慣例，或 `Include` 時直接給條件 | 沒有全域模型也能 Join |
| 跨資料庫 | 不支援 | 同一台伺服器時輸出三段式名稱 | 3.x 既有能力 |
| 預設 hint | 無 | 依設定預設 `WITH (NOLOCK)`（見決策 D4） | 延續團隊習慣 |
| 閉包變數 | 一律參數化 | 數值、布林值以字面值內嵌；其他型別參數化（D5） | 避免參數探查 |
| `DateTime.Now`／`Today` | 翻譯成 `GETDATE()` 等伺服器函式 | 在用戶端求值成參數；要伺服器時間時明確寫 `ChefFunctions.ServerNow()`（D11） | 與一般 C# 變數的直覺一致 |
| 模型與資料表 | 模型必須完整對齊資料表 | 類別可以只描述資料表的一部分；`[Required]` 決定 Insert 欄位（D3） | 以類別為主，不被資料表綁死 |
| 寫入 | `SaveChanges` 為主、`ExecuteUpdate` 為輔 | 只有明確寫入：`Insert`／`ExecuteUpdate`／`ExecuteDelete`／`Upsert`／`Bulk*` | 不追蹤變更 |
| Upsert／Bulk | 無（需第三方） | 內建 | 3.x 既有能力 |

---

## 5. 目標架構

### 5.1 套件與專案配置

| 套件 | 內容 | 相依 |
|---|---|---|
| **Chef.DbAccess** | 公開抽象（`IDataAccess<T>`、`IDataAccessFactory`、屬性）、Metadata、查詢管線、SQL AST、ANSI SQL 產生器、Shaper、非同步運算子、寫入的 AST 與命令建構、Raw SQL 執行器（`@name`／`{=name}`／`IN @list`）、診斷 | `System.ComponentModel.Annotations`、`Microsoft.Bcl.AsyncInterfaces` |
| **Chef.DbAccess.SqlServer** | SQL Server 方言：SQL 產生器、型別對應、翻譯器、連線與交易、`SqlBulkCopy`、Upsert 策略、全文檢索、`SqlServerDataAccessFactory` | Chef.DbAccess、`Microsoft.Data.SqlClient`（不再相依 Dapper，D7） |
| **Chef.DbAccess.Extensions.DependencyInjection** | `AddChefDbAccess(...)` | `Microsoft.Extensions.DependencyInjection.Abstractions` |
| Chef.DbAccess.Sqlite（5.x，選用） | SQLite 方言：驗證抽象、加速測試 | `Microsoft.Data.Sqlite` |

所有套件都只以 **`netstandard2.0`** 為目標（D6）：

- .NET Standard 2.0 是規格而不是執行環境，沒有停止支援日期；.NET Framework 4.6.2 以上與 .NET Core 2.0 之後的每一版 .NET（包括未來的版本）都能直接使用，應用程式升級 .NET 時 Chef 不必重新編譯或改版。
- 如果改成 `net8.0` 之類的特定版本，雖然在之後的執行環境上一樣能跑，但該版本停止支援後，SDK 會開始對它發出警告、相依套件也會陸續拿掉它，最後還是得改版。
- 新版 .NET 才有的功能一律在**執行期偵測**，不靠編譯期的目標框架：`DateOnly`／`TimeOnly` 以型別名稱辨識並對應到 `date`／`time`；.NET 10 的 `Queryable.LeftJoin` 以 `MethodInfo` 名稱與宣告型別辨識；舊執行環境由 Chef 提供 `LeftJoin`（獨立命名空間 `Chef.DbAccess.Linq`，避免與 .NET 10 的同名方法衝突）。
- `Microsoft.Data.SqlClient` 選擇仍提供 `netstandard2.0` 組件的穩定版作為最低版本（目前 6.1 以上）；應用程式可以自行參考更新的版本。Chef 只使用 `SqlConnection`、`SqlCommand`、`SqlParameter`、`SqlBulkCopy`、`SqlException` 等穩定 API，降低跨版本的風險。
- 測試與範例是應用程式，不是函式庫，所以目標設為 `net10.0;net48`，同時驗證 .NET 與 .NET Framework 兩種執行環境。

```
src/Chef.DbAccess/
├─ Chef.DbAccess/
│  ├─ IDataAccess.cs、IDataAccessFactory.cs、ConnectionStringAttribute.cs、ChefFunctions.cs
│  ├─ Metadata/           EntityType、Property、Key、Navigation、ForeignKey、EntityTypeBuilder、Conventions/
│  ├─ Storage/            DataSource、TypeMapping、TypeMappingSource、IRelationalConnection、ChefTransaction、ExecutionContext、RawSql/（RawSqlParser、RawSqlCommand）
│  ├─ Query/
│  │  ├─ ChefQueryProvider.cs、QueryCompiler.cs、QueryCache.cs、ParameterExtractor.cs、ExpressionShapeComparer.cs
│  │  ├─ Preprocessing/   QueryNormalizer、NavigationExpander、IncludeExpression
│  │  ├─ Translation/     QueryableMethodTranslator、SqlTranslator、SqlExpressionFactory、Translators/（介面與共用翻譯器）
│  │  ├─ SqlExpressions/  SelectExpression、TableExpression、JoinExpression…（§5.4.4）
│  │  ├─ Postprocessing/  NullSemanticsProcessor、ParameterBasedSqlProcessor、SqlTreePruner
│  │  ├─ Sql/             QuerySqlGenerator（ANSI）、CommandCache、ChefCommand
│  │  └─ Shaping/         ShaperCompiler、CollectionShaper、QueryingEnumerable
│  ├─ Update/             UpdateExpression、DeleteExpression、InsertCommandBuilder、UpsertCommandBuilder、SetterBuilder
│  ├─ Diagnostics/        ChefEventId、DiagnosticSource、ExpressionPrinter、ChefQueryTranslationException
│  └─ Extensions/         ChefQueryableExtensions（ToListAsync、Include、WithoutNoLock…）、DataAccessWriteExtensions
├─ Chef.DbAccess.SqlServer/
│  ├─ SqlServerDataAccess.cs、SqlServerDataAccessFactory.cs
│  ├─ Query/              SqlServerQuerySqlGenerator、Translators/（String、DateTime、Math、Convert、FullText…）
│  ├─ Storage/            SqlServerTypeMappingSource、SqlServerConnection
│  └─ Update/             SqlServerUpsertStrategy、SqlServerBulkCopy、SqlServerOutputClause
├─ Chef.DbAccess.Extensions.DependencyInjection/
├─ Chef.DbAccess.Tests/                 （不需要資料庫：AST、產生器、翻譯、快取、Shaper）
├─ Chef.DbAccess.SqlServer.Tests/       （SQL 快照測試＋整合測試）
├─ Chef.DbAccess.Benchmarks/
└─ Directory.Build.props、Directory.Packages.props
```

### 5.2 分層圖

```
┌──────────────────────────── 使用者程式碼 ────────────────────────────┐
│ members.Where(...).Select(...).ToListAsync()                          │
│ members.Where(...).ExecuteUpdateAsync(...) / members.InsertAsync(...) │
└───────────────────────────────┬──────────────────────────────────────┘
                                ▼
┌──────────────────────── Chef.DbAccess（核心）────────────────────────┐
│ IDataAccess<T> : IOrderedQueryable<T>, IAsyncEnumerable<T>            │
│        │ Provider = ChefQueryProvider                                 │
│ ┌──────▼─────────────── QueryCompiler ───────────────────────────┐   │
│ │ ① ParameterExtractor     閉包／可求值子樹 → 參數                 │   │
│ │ ② QueryCache             形狀雜湊命中 → 跳到 ⑨                   │   │
│ │ ③ QueryNormalizer        查詢語法、LeftJoin、透明識別項           │   │
│ │ ④ NavigationExpander     導覽 → JOIN／EXISTS；Include            │   │
│ │ ⑤ QueryableMethodTranslator ＋ SqlTranslator                    │   │
│ │      → ShapedQueryExpression(SelectExpression, Shaper)         │   │
│ │ ⑥ SqlTreePruner、Pushdown 清理                                  │   │
│ │ ⑦ ShaperCompiler         Shaper → Func<DbDataReader, T>          │   │
│ └──────┬─────────────────────────────────────────────────────────┘   │
│ ┌──────▼─────────────── 執行期（每次執行）─────────────────────────┐   │
│ │ ⑧ CommandCache：依參數 null／集合長度                            │   │
│ │      NullSemanticsProcessor → ParameterBasedSqlProcessor        │   │
│ │      → QuerySqlGenerator（方言）→ ChefCommand（SQL＋參數繫結）  │   │
│ │ ⑨ 建立 DbCommand、套用參數值 → DbDataReader → Shaper            │   │
│ │    → QueryingEnumerable<T>（同步／非同步串流）                   │   │
│ └────────────────────────────────────────────────────────────────┘   │
│ Metadata（EntityTypeCache<T>）  TypeMappingSource  TranslatorRegistry │
└───────────────────────────────▲──────────────────────────────────────┘
                                │ 方言服務（IDatabaseProvider）
┌──────────────────── Chef.DbAccess.SqlServer ─────────────────────────┐
│ SqlServerQuerySqlGenerator：[]、TOP、OFFSET/FETCH、hint、OPTION、     │
│   三段式名稱、OUTPUT、MERGE                                           │
│ SqlServerTypeMappingSource：varchar／nvarchar／datetime2／decimal…    │
│ Translators：LEN、CHARINDEX、DATEADD、DATEPART、CONTAINS、FREETEXT…   │
│ 連線／交易、SqlBulkCopy、Upsert 策略、暫時性錯誤重試                  │
└──────────────────────────────────────────────────────────────────────┘
```

### 5.3 一次查詢的執行流程

1. `members.Where(x => x.Age > age).ToListAsync()`：`Where` 由 `System.Linq.Queryable` 建立 `MethodCallExpression`；`ToListAsync` 呼叫 `ChefQueryProvider.ExecuteAsync<IAsyncEnumerable<Member>>`。
2. `ParameterExtractor` 走訪運算式：`age` 是閉包欄位 → 換成 `QueryParameterExpression("age")`，值放進 `QueryContext.Parameters`，並依型別標記為「內嵌」（`int`）或「參數」（§5.7）；遇到閉包裡的 `IQueryable`（Chef 的 provider）→ 以其 `Expression` 取代，形成子查詢。
3. 以處理後的運算式＋`DataSource` 身分＋方言＋hint 計算 `QueryCacheKey`，查 `QueryCache`（有上限的 LRU）。
4. 沒命中：依序執行 ③～⑦，得到 `CompiledQuery`（`SelectExpression` 範本＋編譯好的 Shaper＋參數描述）。
5. 執行期：`CommandCache` 以「哪些值是 null、集合長度落在哪個區間」為鍵，取得或產生 SQL 範本（片段＋插槽），再把內嵌的值填進插槽得到最終 SQL 字串（§5.7.2）。
6. 取得連線（環境交易中則沿用其連線），建立 `DbCommand`、繫結參數、`ExecuteReaderAsync`，Shaper 逐列產生物件，`QueryingEnumerable` 串流回傳。
7. 呼叫 `OutputSql`、寫 `ILogger`、送 `DiagnosticSource` 事件；發生例外時呼叫 `OnDbError` 後重新拋出。

### 5.4 核心型別設計

#### 5.4.1 公開介面

```csharp
namespace Chef.DbAccess;

public interface IDataAccess<T> : IOrderedQueryable<T>, IAsyncEnumerable<T>
{
    /// 連線、伺服器、資料庫、資料表、結構描述
    DataSource Source { get; }

    /// 這個查詢根的預設設定（NOLOCK、逾時…）；會納入查詢快取鍵
    DataAccessOptions Options { get; }

    /// 3.x 掛鉤，保留
    Action<string, object> OutputSql { get; set; }
    Action<Exception, string, object> OnDbError { get; set; }
}

public interface IDataAccessFactory
{
    IDataAccess<T> Create<T>();
    IDataAccess<T> Create<T>(string tableName);
    IDataAccess<T> Create<T>(string tableName, string nameOrConnectionString);
    void AddConnectionString(string name, string value);
    ValueTask<ChefTransaction> BeginTransactionAsync(string nameOrConnectionString, IsolationLevel isolationLevel = IsolationLevel.ReadCommitted, CancellationToken cancellationToken = default);
    Action<Exception, string, object> OnDbError { get; set; }
}
```

- `IsDirtyRead` 由 `Options.DirtyRead` 取代。
- 寫入、非同步運算子、Raw SQL 全部是擴充方法（`DataAccessWriteExtensions`、`ChefQueryableExtensions`），透過內部的 `IDataAccessInternal`（取得 `IDatabaseProvider`、`EntityType`、執行器）實作。介面因此可以長期穩定。

#### 5.4.2 查詢根與 Provider

```csharp
public sealed class DataAccessQueryRootExpression : Expression
{
    public EntityType EntityType { get; }
    public DataSource Source { get; }         // 伺服器＋資料庫＋資料表＋結構描述
    public QueryHints Hints { get; }          // NOLOCK、其他 table hint
    public override Type Type => typeof(IQueryable<>).MakeGenericType(EntityType.ClrType);
    public override ExpressionType NodeType => ExpressionType.Extension;
}

internal sealed class ChefQueryProvider : IAsyncQueryProvider   // Chef 自己定義的 IAsyncQueryProvider
{
    IQueryable<TElement> CreateQuery<TElement>(Expression expression);   // 回傳 ChefQueryable<TElement>
    TResult Execute<TResult>(Expression expression);
    TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken);
}
```

非同步終結運算子（`ToListAsync`、`FirstAsync`…）的實作方式比照 EF Core：以 `QueryableMethods` 的 `MethodInfo` 包一層 `MethodCallExpression`，再交給 `IAsyncQueryProvider.ExecuteAsync`；`IQueryable` 不是 Chef 的 provider 時丟出明確例外。

#### 5.4.3 QueryCompiler 與快取

```csharp
internal sealed class QueryCompiler
{
    public TResult Execute<TResult>(Expression query, bool async, CancellationToken ct)
    {
        var context = new QueryContext(ct);
        var extracted = parameterExtractor.Extract(query, context.Parameters);
        var key = new QueryCacheKey(extracted, provider.Id, async);
        var compiled = queryCache.GetOrAdd(key, _ => CompileCore<TResult>(extracted, async));
        return compiled(context);
    }
}
```

- `QueryCache`：預設上限 4,096 筆、LRU 淘汰；提供 `ChefDbAccessOptions.QueryCacheSize`。
- 快取鍵包含：運算式形狀（不含參數值）、`DataSource` 的伺服器與資料庫（決定是否輸出三段式名稱）、方言、hint、async 旗標。
- `ChefQuery.Compile`／`CompileAsync`：使用者明確預先編譯，直接跳過形狀比較。

#### 5.4.4 SQL AST

| 類別 | 節點 |
|---|---|
| 查詢 | `SelectExpression`（Projection、Tables、Predicate、GroupBy、Having、Orderings、Offset、Limit、IsDistinct、Tags）、`ProjectionExpression`、`OrderingExpression` |
| 來源 | `TableExpression`（含資料庫、結構描述、hint）、`FromSqlExpression`、`InnerJoinExpression`、`LeftJoinExpression`、`CrossJoinExpression`、`CrossApplyExpression`、`OuterApplyExpression`、`UnionExpression`、`ExceptExpression`、`IntersectExpression` |
| 純量 | `ColumnExpression`、`SqlConstantExpression`、`SqlParameterExpression`、`SqlBinaryExpression`、`SqlUnaryExpression`、`SqlFunctionExpression`、`CaseExpression`、`LikeExpression`、`InExpression`、`ExistsExpression`、`ScalarSubqueryExpression`、`SqlFragmentExpression`、`RowNumberExpression`（5.x） |
| 寫入 | `UpdateExpression`（`ColumnValueSetter` 清單）、`DeleteExpression`、`InsertExpression`（Chef 自加）、`OutputClause`（Chef 自加）、`MergeExpression`（SQL Server，Chef 自加） |

每個 `SqlExpression` 都帶 `TypeMapping`，所以參數與常數輸出時知道正確的資料庫型別。AST 節點**不可變**，修改一律回傳新節點（`Update(...)` 模式），以便快取範本可以安全地共用。

#### 5.4.5 翻譯器擴充點

```csharp
public interface IMethodCallTranslator
{
    SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments, ISqlExpressionFactory sql);
}

public interface IMemberTranslator
{
    SqlExpression Translate(SqlExpression instance, MemberInfo member, Type returnType, ISqlExpressionFactory sql);
}

public interface IAggregateMethodCallTranslator { ... }

// 使用者自訂
ChefDbAccess.Configure(o => o.AddMethodCallTranslator(new MyJsonValueTranslator()));

// 也可以用屬性對應純量函式
public static class Db
{
    [DbFunction("fn_CalcScore", Schema = "dbo")]
    public static int CalcScore(int age, string name) => throw new NotSupportedException();
}
```

### 5.5 Metadata：沒有 DbContext 的模型

`EntityType` 在型別第一次被使用時解析，存在 `static class EntityTypeCache<T>`（泛型靜態欄位，零查找成本），同時以 `ConcurrentDictionary<Type, EntityType>` 供非泛型路徑使用。

| 來源 | 對應 |
|---|---|
| `[Table(Name, Schema)]` | 資料表、結構描述；沒有時用類別名稱、`dbo`；`Create<T>(tableName)` 可以覆寫 |
| `[Column(Name, TypeName, Order)]` | 欄位名稱、資料庫型別（`varchar`、`date`、`datetime2(3)`、`decimal(18,4)`…） |
| `[Key]`（可多個＋`[Column(Order)]` 組成複合鍵） | 主鍵；沒有時依慣例找 `Id` 或 `<類別名稱>Id` |
| `[Required]` | **決定 `InsertAsync(entity)`、`InsertAsync(IEnumerable<T>)`、`BulkInsertAsync`、`UpsertAsync(entity)` 的 INSERT 部分要寫入哪些欄位**（延續 3.x，D3）；沒有任何 `[Required]` 屬性時丟出例外，提示改用 setter 寫法。只有這個用途，不影響查詢的 null 語意 |
| `[DatabaseGenerated(Identity / Computed)]` | 只用於回填與防呆：`returning:` 沒指定時 Identity 欄位不會自動回填；Identity／Computed 屬性同時標了 `[Required]` 時丟出例外 |
| CLR 型別是否可為 null | null 語意與 JOIN 型態依屬性型別判斷：`int` 外鍵 → `INNER JOIN`，`int?` 外鍵 → `LEFT JOIN`；`string` 視為可為 null |
| `[StringLength]`／`[MaxLength]` | 參數長度（超過時不設定長度，避免被 SqlClient 截斷） |
| `[NotMapped]` | 排除；在查詢中使用時丟出明確例外 |
| `[ForeignKey]`、`[InverseProperty]` | 導覽屬性 |
| 命名慣例 | 屬性 `Department`（實體型別）＋屬性 `DepartmentId` → 參考導覽；可以在選項中關閉 |
| `[ConnectionString(name)]`（Chef） | 連線解析；3.x 的 `Schema` 參數移除，改用 `[Table(Schema = ...)]` |
| `[HasTrigger]`（Chef 新增） | 寫入時改用 `OUTPUT ... INTO @table`，避免觸發程序造成 `OUTPUT` 失敗 |

類別只需要描述「這個用途用得到的欄位」：資料表有 30 個欄位、類別只有 5 個屬性完全沒問題；同一張表可以有多個類別（例如 `Member`、`MemberSummary`、`MemberForImport`），各自用 `[Table]` 指向同一張表、各自決定 `[Required]`。

無法加屬性的型別（例如共用 DTO 組件）可以用全域 Fluent 設定，**不需要 DbContext**：

```csharp
ChefDbAccess.Entity<Member>(b =>
{
    b.ToTable("tbl_member", "dbo");
    b.HasKey(x => x.Id);
    b.Property(x => x.Id).HasColumnName("No");
    b.Property(x => x.Phone).HasColumnType("varchar(20)");
    b.Property(x => x.Name).IsRequired();   // 等同 [Required]：Insert 時寫入
    b.Ignore(x => x.Secret);
    b.HasOne(x => x.Department).WithForeignKey(x => x.DepartmentId);
    b.HasMany(x => x.ManagedDepartments).WithForeignKey<Department>(d => d.ManagerId);
});
```

全域設定必須在第一次使用該型別之前完成；之後再設定會丟出例外（避免快取不一致）。

### 5.6 型別對應（TypeMapping）

`TypeMappingSource` 依「CLR 型別＋`[Column(TypeName)]`＋`[StringLength]`＋Unicode 與否」決定 `TypeMapping`，每個對應知道：資料庫型別字串、`DbType`／`SqlDbType`、大小／精確度、是否預設內嵌（§5.7）、如何產生字面值、如何從 `DbDataReader` 讀值（`GetInt32`、`GetString`、`GetFieldValue<T>`）。

| CLR | 預設 SQL Server 型別 | 預設處理 | 備註 |
|---|---|---|---|
| `bool` | `bit` | 內嵌（`1`／`0`） | 在投影中輸出 `CAST(1 AS bit)` |
| `byte`、`sbyte`、`short`、`ushort`、`int`、`uint`、`long`、`ulong` | 對應的整數型別 | 內嵌 | |
| `float`、`double` | `real`、`float` | 內嵌（round-trip 格式＋`E0`） | `NaN`／`Infinity` 丟出例外 |
| `decimal` | `decimal(18, 2)` | 內嵌（完整精確度） | 建議用 `[Column(TypeName = "decimal(18,4)")]` 或 `[Precision]` 明確指定 |
| 以整數儲存的列舉 | 底層整數型別 | 內嵌底層整數值 | 字串列舉為 5.x 的 value converter |
| `string` | `nvarchar(max)`；有 `[StringLength(n)]` 時 `nvarchar(n)` | 參數 | `TypeName = "varchar"` → `varchar(n)`，參數也是 `varchar`（取代 3.x 的 `DbString`） |
| `DateTime` | `datetime2` | 參數 | `TypeName = "datetime"`／`"date"` 時依指定 |
| `DateOnly`／`TimeOnly`（執行期偵測） | `date`／`time` | 參數 | |
| `Guid`、`DateTimeOffset`、`TimeSpan`、`byte[]` | 對應的原生型別 | 參數 | |

以上型別「可為 null 的版本」（`int?`、`bool?`…）有值時同樣內嵌；**值為 null 時一律不內嵌**，改走一般 null 值的處理方式（§5.7.3）。

5.x 再加入 Value Converter（列舉存字串、JSON 欄位、自訂型別）。

### 5.7 字面值內嵌、參數化、快取與 Null 語意

#### 5.7.1 值的處理規則

| 運算式中的值 | 處理方式 |
|---|---|
| 字面常數 `x.Age > 30`、`"J"` | SQL 常數，屬於運算式形狀的一部分 |
| 閉包變數與可在用戶端求值的子樹（`x.Age > age`、`GetId()`），型別是 `bool`、整數、浮點、`decimal`、以整數儲存的列舉（含 `Nullable`），而且**值不是 null** | **字面值內嵌**（預設，延續 3.x 的 `{=}`）：`[m].[Age] > 30` |
| 同上，但值是 null | 不內嵌，依一般 null 值處理（§5.7.3） |
| 同上，其他型別（`string`、`DateTime`、`Guid`、`byte[]`…） | 參數，型別由比較的欄位推導（`varchar` 欄位 → `varchar` 參數） |
| `DateTime.Now`／`UtcNow`／`Today`、`DateTimeOffset.Now`… | 在用戶端求值成參數（D11） |
| `ChefFunctions.ServerNow()`／`ServerUtcNow()`／`ServerToday()`／`ServerSysDateTime()` | 伺服器端求值：`GETDATE()`／`GETUTCDATE()`／`CONVERT(date, GETDATE())`／`SYSDATETIME()`（D11） |
| `Skip(n)`、`Take(n)`、`First`／`Single` 的 `TOP` | 整數，所以內嵌：`OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY` |
| `ChefFunctions.Parameter(value)` | 對這個值強制參數化（例如明確想共用執行計畫的熱門查詢） |
| `ChefFunctions.Constant(value)` | 對這個值強制內嵌，可用於字串、日期等（由 `TypeMapping` 產生安全的字面值，字串的 `'` 會跳脫） |

內嵌的範圍和 3.x 一致：`WHERE`／`JOIN`／`HAVING` 條件、`CASE`、`SET`（`ExecuteUpdateAsync`）、`VALUES`（`InsertAsync`）、`TOP`／`OFFSET`／`FETCH`、`IN` 清單，以及 Raw SQL 的 `{=name}` 與 `FromSql` 插值。

全域選項：

```csharp
ChefDbAccess.Configure(o =>
{
    o.ValueInlining = ValueInlining.NumericAndBoolean;     // 預設；None = 全部參數化
    o.DateTimeNowEvaluation = DateTimeNowEvaluation.Client; // 預設；Server = DateTime.Now 一律翻成 GETDATE()
});
```

單一查詢也可以 `.WithValueInlining(ValueInlining.None)`。這兩個選項都會納入查詢快取鍵。

#### 5.7.2 內嵌如何與快取共存

字面值內嵌代表「每個值都是不同的 SQL 文字」，但**翻譯與 SQL 產生仍然只做一次**：

1. 查詢快取鍵只看運算式形狀，不含值，所以 `x.Age > age` 不論 `age` 是多少都命中同一筆快取。
2. SQL 產生器在要內嵌的位置輸出「插槽」，`ChefCommand` 保存為「SQL 片段陣列＋插槽描述（對應的值、`TypeMapping`）」，例如 `["… WHERE [m].[Age] > ", " ORDER BY …"]`＋`[slot(age, int)]`。
3. 每次執行時，用插槽的 `TypeMapping` 把值格式化成字面值，與片段串接成最終 SQL。這一步只是字串串接（預先算好長度、一次配置），不重跑翻譯也不重跑產生器。
4. `OutputSql`、`ToQueryString()`、`ILogger` 看到的都是填好值的最終 SQL。

**格式化規則（同時是防注入的保證）**：只有 §5.6 標為「內嵌」的型別會走這條路；一律 `CultureInfo.InvariantCulture`；整數直接輸出；`decimal` 輸出完整精確度；`float`／`double` 用 round-trip 格式並加 `E0` 確保 SQL Server 視為浮點數；`bool` 輸出 `1`／`0`；`NaN`／`Infinity` 丟出例外。值的型別在執行期再檢查一次（例如 `object` 型別的閉包變數），不是上述型別就改走參數。格式化器**永遠不會輸出 `NULL`**：null 值在選擇 SQL 範本時就已經離開內嵌路徑（§5.7.3）。

**取捨**：每個不同的值都會讓 SQL Server 編譯並快取一份執行計畫，這正是避免參數探查要付的代價。文件中建議：

- 資料庫開啟 `optimize for ad hoc workloads`，降低只執行一次的計畫占用的記憶體。
- 資料庫不要設定 `PARAMETERIZATION FORCED`，否則 SQL Server 會把字面值再轉回參數，內嵌就失去作用。
- 執行頻率極高、值的分布又平均的查詢，可以用 `ChefFunctions.Parameter(...)` 或 `.WithValueInlining(ValueInlining.None)` 改回參數。

#### 5.7.3 值為 null 時

`int?`、`bool?` 這類可內嵌型別的值是 null 時，**處理方式和 `string` 等非內嵌型別的 null 值完全相同**，不會因為型別可內嵌就有不同行為：

| 位置 | 產生的 SQL | 與 3.x 比較 |
|---|---|---|
| `==` 比較（`x.ManagerId == managerId`） | `[m].[ManagerId] IS NULL` | 相同 |
| `!=` 比較 | `[m].[ManagerId] IS NOT NULL` | 相同 |
| `>`、`<` 等大小比較 | 依 C# 語意（與 null 比大小恆為 false）輸出 `1 = 0`；`NOT` 之下由 null 語意正確反轉 | 3.x 丟出 `ArgumentException` |
| `SET`（`ExecuteUpdateAsync`）、`VALUES`（`InsertAsync`） | 參數 `@p`，值為 `DBNull.Value`，型別依欄位的 `TypeMapping` | 結果相同（都寫入 NULL） |
| `IN` 清單（`ids.Contains(x.ManagerId)`，`ids` 含 null） | null 移出清單，另外補 `OR [m].[ManagerId] IS NULL` | 3.x 不支援 |
| Raw SQL 的 `{=name}` | 改成 `@name` 參數，繫結 `DBNull.Value`；Raw SQL 不分析語意，`= @name` 遇到 NULL 會不成立，與直接寫 `@name` 時相同 | 3.x 經由 Dapper 輸出字面值 `null` |

實作方式：`CommandCache` 的鍵包含「每個值是否為 null」，所以同一個形狀會依 null 的組合各自產生、快取一份 SQL 範本。null 的那一份範本在該位置是 `IS NULL` 或參數，**不是插槽**，所以它的 SQL 文字固定，可以重用執行計畫。

#### 5.7.4 Null 語意

`NullSemanticsProcessor`（參考 `SqlNullabilityProcessor`）：

| C# | 產生的 SQL |
|---|---|
| `x.Name == name`，`name` 是 null | `[m].[Name] IS NULL`（在執行期依值是否為 null 選擇 SQL 範本，見 §5.7.3） |
| `x.Name != name`，欄位可為 null | `[m].[Name] <> @name OR [m].[Name] IS NULL` |
| `x.ManagerId == x.DeputyId`（兩邊都可為 null） | `[m].[ManagerId] = [m].[DeputyId] OR ([m].[ManagerId] IS NULL AND [m].[DeputyId] IS NULL)` |
| `!(x.Age > 30)`，欄位可為 null | 正確補上 null 判斷 |

可以透過 `ChefDbAccessOptions.UseRelationalNulls = true` 關閉補償，直接輸出 SQL 原生語意。

### 5.8 集合參數（`Contains`）策略

```csharp
var ids = new[] { 1, 2, 3 };
members.Where(x => ids.Contains(x.Id));
```

預設策略依元素型別（是否屬於 §5.6 的內嵌型別）與數量決定：

| 元素型別 | 數量 | 預設 SQL | 說明 |
|---|---|---|---|
| 數值、布林、整數列舉 | ≤ 門檻（預設 1,000） | `[m].[No] IN (1, 2, 3)` | 字面值內嵌，與 D5 一致；不占用參數，不受 2,100 個參數上限限制 |
| 其他（字串、`Guid`、日期） | ≤ 門檻，且剩餘參數足夠 | `[m].[Code] IN (@codes1, @codes2, @codes3)` | 元素數補齊到桶（1、2、5、10、20、50、100、200、500、1000）並重複最後一個值，讓 SQL 範本可以重用 |
| 任何型別 | > 門檻，或剩餘參數不足 | `[m].[No] IN (SELECT [value] FROM [#chef_…])` | 先以 `SqlBulkCopy` 把值寫入連線範圍的暫存資料表，再執行查詢，最後 `DROP`；與 Bulk 共用 §5.12.3 的機制，同樣不留痕跡 |

不使用 `OPENJSON`、`STRING_SPLIT`：兩者都需要資料庫相容性層級 130，不符合 §5.16 的支援基準（SQL Server 2016 SP1，任何相容性層級）。

- 暫存資料表模式多兩趟往返，但不受參數上限限制，也避免超長 `IN` 清單拖慢編譯或觸發錯誤 8623；`value` 欄位加上主鍵（值在用戶端先去除重複），讓 SQL Server 有正確的基數與排序資訊。
- 查詢以 `AsAsyncEnumerable()` 串流時，暫存資料表在列舉結束（`DisposeAsync`）時才刪除。
- **支援 C# 14 的 first-class span（D15）**：專案以 .NET 10／C# 14 編譯時，運算式樹中的 `ids.Contains(x.Id)` 會綁定到 `MemoryExtensions.Contains(ReadOnlySpan<T>, T)`，集合外面再包一層 `Span<T>`／`ReadOnlySpan<T>` 的 `op_Implicit` 呼叫，而不是 `Enumerable.Contains`。span 是 ref struct，無法裝箱，也無法以 `Expression.Compile()` 求值，所以 `ParameterExtractor` 必須在求值**之前**剝掉 `op_Implicit`，把呼叫改寫回 `Enumerable.Contains`；`MemoryExtensions.SequenceEqual` 同樣改寫。帶第三個參數（比較子）的多載不改寫，視為無法翻譯。作法比照 EF Core 10 的 `ExpressionTreeFuncletizer`。3.x 不支援這種形狀，也不會修正。
- 空集合輸出 `1 = 0`；集合含 null 時，null 移出清單並另外補 `OR [m].[No] IS NULL`（§5.7.3）。
- 可以用 `CollectionTranslationMode`（`Constant`／`MultipleParameters`／`TempTable`）在全域、單一查詢（`.WithCollectionMode(...)`）或單一運算式（`ChefFunctions.Constant(ids)`／`ChefFunctions.Parameter(ids)`）覆寫，門檻也可以調整。

### 5.9 用戶端求值規則

- **只有最外層投影**可以包含無法翻譯的部分，例如 `Select(x => new Dto { Display = Format(x.Name, x.Age) })`：Chef 只查出 `Name`、`Age`，再在用戶端呼叫 `Format`。
- 其他位置（`Where`、`OrderBy`、`GroupBy`、Join 條件、子查詢內部）無法翻譯時，丟出 `ChefQueryTranslationException`，訊息包含 `ExpressionPrinter` 印出的運算式，以及建議（「請改用 `AsEnumerable()` 明確切到用戶端」）。
- `AsEnumerable()`／`ToList()` 之後的運算子屬於 LINQ to Objects，與 Chef 無關。

### 5.10 Join、導覽屬性與 Include

| 寫法 | 翻譯 |
|---|---|
| `Join` | `INNER JOIN`，支援複合鍵（匿名型別鍵） |
| `GroupJoin` ＋ `SelectMany` ＋ `DefaultIfEmpty` ／ `LeftJoin` | `LEFT JOIN`；右側沒有資料時選出的實體為 `null` |
| 兩個不相關的 `from` | `CROSS JOIN` |
| 相關的 `SelectMany`（`m => departments.Where(d => d.ManagerId == m.Id)`） | `CROSS APPLY`；搭配 `DefaultIfEmpty` → `OUTER APPLY`（能化簡成 JOIN 時化簡） |
| `x.Department.Name`（參考導覽） | 自動 JOIN，外鍵不可為 null → `INNER JOIN`，可為 null → `LEFT JOIN`；同一條導覽路徑在同一個查詢只 JOIN 一次 |
| `x.ManagedDepartments.Any(...)`、`.Count()`（集合導覽） | `EXISTS (...)`、純量子查詢 |
| `Include(x => x.Department)` | JOIN 並填回導覽屬性 |
| `Include(x => x.ManagedDepartments)` | `LEFT JOIN`＋依父資料主鍵 `ORDER BY`，以主鍵 identity resolution 合併；`ThenInclude` 支援多層 |
| `Include(nav, (x, y) => 條件)` | 3.x 寫法：沒有任何關聯標註時直接給 JOIN 條件 |
| `Include(...).Where(...)`（filtered include） | 5.0 支援 `Include(x => x.ManagedDepartments.Where(d => ...))` |

- Join 的兩個來源必須在**同一台伺服器**；不同資料庫時輸出 `[db].[schema].[table]`；不同伺服器丟出 `InvalidOperationException`。
- `Include` 搭配 `Skip`／`Take`／`First` 時，先在子查詢中對父資料分頁，再 JOIN 子資料（避免分頁被一對多放大）。
- 一對多 Include 的笛卡兒爆炸問題：5.0 在同一層同時 Include 兩個以上集合時記錄警告；`AsSplitQuery()` 列為 5.x。

### 5.11 子查詢與 Pushdown 規則

`QueryableMethodTranslator` 在套用運算子之前檢查目前的 `SelectExpression` 狀態，必要時先把它包成子查詢（pushdown），規則如下：

| 已經有 | 接著要套用 | 動作 |
|---|---|---|
| `Limit`／`Offset` | `Where`、`OrderBy`、`Distinct`、`Join`、`GroupBy`、聚合 | 包成子查詢 |
| `Distinct` | `OrderBy`（非投影欄位）、`Skip`／`Take`、`Join`、`GroupBy` | 包成子查詢 |
| `GroupBy` | `Where`（非聚合）、`Join`、`Select` 進一步組合 | `Having` 或包成子查詢 |
| 集合運算（Union…） | 任何運算子 | 包成子查詢 |
| 投影含純量子查詢或計算欄位 | `Where`／`OrderBy` 用到該欄位 | 包成子查詢或直接重複運算式 |

子查詢中的 `ORDER BY` 若沒有 `TOP`／`OFFSET` 則移除（SQL Server 不允許）。

### 5.12 寫入管線

#### 5.12.1 set-based：`ExecuteUpdateAsync`／`ExecuteDeleteAsync`

- 先把來源 `IQueryable` 翻譯成 `SelectExpression`，再轉成 `UpdateExpression`／`DeleteExpression`；可以包含 JOIN（導覽屬性、`Join`）與子查詢條件。
- setter 支援兩種寫法：
  - Chef 風格 member-init：`x => new Member { Age = x.Age + 1, Phone = phone }`（`Expression<Func<T, T>>`，也接受無參數的 `() => new Member { ... }`）。
  - EF Core 風格：`s => s.SetProperty(x => x.Age, x => x.Age + 1)`。
- 可選 `returning:` → `OUTPUT INSERTED.*`／`DELETED.*`，回傳 `List<TResult>`。

```sql
UPDATE [m]
SET [m].[Phone] = @phone, [m].[Age] = [m].[Age] + 1
FROM [tbl_member] AS [m]
INNER JOIN [tbl_department] AS [d] ON [m].[DepartmentId] = [d].[Id]
WHERE [d].[Name] LIKE N'%業務%';
```

#### 5.12.2 實體寫入：Insert／Upsert

| API | SQL Server 策略 |
|---|---|
| `InsertAsync(entity)` | `INSERT INTO ... ([Required] 欄位) VALUES (...)`（D3）；數值、布林值內嵌，其他參數化；需要回填時明確指定 `returning:` |
| `InsertAsync(() => new T { ... })` | 只寫入指定欄位 |
| `InsertAsync(IEnumerable<T>)` | 多列 `VALUES`（`[Required]` 欄位），依「每批 1,000 列」與「2,100 參數上限」（內嵌的值不占參數）自動分批，整體包在一個交易中 |
| `InsertAsync(..., whenNotExists:)` | `INSERT ... SELECT ... WHERE NOT EXISTS (...)` |
| `UpsertAsync(entity, on:, update:)` | 預設：交易中 `UPDATE ... WITH (UPDLOCK, SERIALIZABLE)` → `IF @@ROWCOUNT = 0 INSERT`；可選 `MERGE ... WITH (HOLDLOCK)`（決策 D10） |
| `returning:` | 資料表沒有觸發程序：`OUTPUT INSERTED.*`；有 `[HasTrigger]`：`OUTPUT ... INTO @t` 再 `SELECT` |

#### 5.12.3 Bulk

3.x 用「每次 `CREATE TYPE` → TVP → `DROP TYPE`」做到不留下資料庫物件。5.0 的目標相同，但改用**只存在於這條連線的本機暫存資料表（`#chef_<guid>`）＋`SqlBulkCopy`**：

| 比較 | 3.x：`CREATE TYPE`＋TVP＋`DROP TYPE` | 5.0：`#暫存資料表`＋`SqlBulkCopy` |
|---|---|---|
| 留下的痕跡 | 正常時不留；`CREATE` 與 `DROP` 之間程序中斷（逾時、當機、部署重啟）時，型別**殘留**在使用者資料庫 | 不會留下：`#` 資料表只屬於這條連線，Chef 用完後明確 `DROP`；就算程序中斷，連線關閉或被連線池重用（`sp_reset_connection`）時 SQL Server 也會自動刪除 |
| 權限 | 需要使用者資料庫的 `CREATE TYPE` 與結構描述的 `ALTER` 權限 | 不需要額外權限（任何登入都能在 tempdb 建立 `#` 資料表） |
| 對使用者資料庫的影響 | 每次都寫入系統目錄 | 不碰使用者資料庫的結構描述，只在 tempdb |
| 並行 | 型別名稱需要保證不重複 | `#` 資料表天生只有自己的連線看得到，不會衝突 |
| 往返次數 | `CREATE TYPE`、執行、`DROP TYPE` 三趟，外加 TVP 傳輸 | `CREATE TABLE #`、`SqlBulkCopy`（串流）、執行並 `DROP` 三趟；純 Insert 只要一趟 |
| 傳輸 | TVP（`SqlDataRecord` 逐列轉換） | `SqlBulkCopy` 的 TDS bulk 協定，資料量大時明顯較快，可以搭配 `TableLock` 減少 tempdb 記錄 |

另外評估過但不採用的做法：

- **永久的 TVP 型別（只建立一次）**：會在使用者資料庫留下物件，違反「不留痕跡」。
- **`OPENJSON(@json)` 傳整批資料**：需要資料庫相容性層級 130，不符合支援基準（§5.16）；而且大量資料時 JSON 序列化與解析的成本高於 `SqlBulkCopy`。
- **XML 參數＋`nodes()`**：任何版本都能用，但解析成本更高，不採用。

各 API 的流程：

| API | 策略 |
|---|---|
| `BulkInsertAsync` | 沒有 `returning`／`whenNotExists`：`SqlBulkCopy` **直接寫入目標資料表**，不需要暫存資料表（預設開啟 `FireTriggers`、`CheckConstraints`，行為與一般 `INSERT` 相同）；有：`#temp` → `INSERT ... SELECT ... FROM #temp`（`WHERE NOT EXISTS`、`OUTPUT`） |
| `BulkUpdateAsync(values, on:, columns:)` | `#temp` → `UPDATE t SET ... FROM target t INNER JOIN #temp s ON ...` |
| `BulkUpsertAsync` | `#temp` → 交易中 `UPDATE ... WITH (UPDLOCK, SERIALIZABLE) JOIN` ＋ `INSERT ... WHERE NOT EXISTS`（或 `MERGE`，同 D10） |
| `BulkDeleteAsync(values, on:)` | `#temp` → `DELETE t FROM target t INNER JOIN #temp s ON ...` |

`BulkUpsertAsync` 的完整流程（同一條連線、同一個交易）：

```sql
CREATE TABLE [#chef_3f2a…] ([No] int NOT NULL, [Name] nvarchar(50) COLLATE DATABASE_DEFAULT NULL, [Phone] varchar(20) COLLATE DATABASE_DEFAULT NULL);
-- SqlBulkCopy 串流寫入 [#chef_3f2a…]
UPDATE [t] SET [t].[Name] = [s].[Name], [t].[Phone] = [s].[Phone]
FROM [tbl_member] AS [t] WITH (UPDLOCK, SERIALIZABLE)
INNER JOIN [#chef_3f2a…] AS [s] ON [t].[No] = [s].[No];
INSERT INTO [tbl_member] ([No], [Name], [Phone])
SELECT [s].[No], [s].[Name], [s].[Phone] FROM [#chef_3f2a…] AS [s]
WHERE NOT EXISTS (SELECT 1 FROM [tbl_member] AS [t] WITH (UPDLOCK, SERIALIZABLE) WHERE [t].[No] = [s].[No]);
DROP TABLE [#chef_3f2a…];
```

實作細節：

- `CREATE TABLE #…` 必須以**沒有參數的命令**送出（SqlClient 會直接當成批次執行）。帶參數的命令會透過 `sp_executesql` 執行，在裡面建立的 `#` 資料表會在 `sp_executesql` 結束時被刪除，後面的 `SqlBulkCopy` 就找不到了。
- 暫存資料表的欄位定義由 Metadata 與 `TypeMapping` 產生（和 3.x 產生 TVP 欄位定義的方式相同），Identity 欄位不帶 `IDENTITY` 屬性；字串欄位加上 `COLLATE DATABASE_DEFAULT`，避免 tempdb 與使用者資料庫定序不同時 JOIN 失敗。
- 寫入哪些欄位：`BulkInsertAsync` 依 `[Required]`（D3）；`BulkUpdateAsync`／`BulkUpsertAsync` 依 `on:` 與 `columns:`／`update:`。
- `SqlBulkCopy` 的 `ColumnMappings` 由 Metadata 產生，資料來源用自寫的 `IDataReader`（`EntityDataReader<T>`）串流讀取物件，不先轉成 `DataTable`。
- 在 `ChefTransaction` 或 `TransactionScope` 中時，`SqlBulkCopy` 使用同一個交易。
- `DROP TABLE` 放在 `finally`；失敗時不影響原本的例外。

#### 5.12.4 Raw SQL（取代 Dapper）

5.0 不再相依 Dapper（D7），`QueryAsync<T>(sql, param)`、`ExecuteAsync(sql, param)`、`FromSql(...)` 由 Chef 自己的 `RawSqlParser`＋`ShaperCompiler` 實作，SQL 字串的寫法與 3.x 相同：

| 語法 | 處理 |
|---|---|
| `@name` | 繫結成參數；型別依值的 CLR 型別推導，可以傳 `ChefParameter.Varchar(value, 20)` 指定 `varchar` |
| `{=name}` | 字面值內嵌，規則與 §5.7.2 相同（只接受內嵌型別，其他型別丟出例外）；值為 null 時改成 `@name` 參數（§5.7.3） |
| `IN @list` | 展開：數值清單內嵌成 `IN (1, 2, 3)`，其他清單展開成 `IN (@list1, @list2, …)`；空清單輸出 `IN (SELECT NULL WHERE 1 = 0)`；大量時同 §5.8 改用 `#暫存資料表` |
| `param` 的來源 | 匿名型別、一般物件、`IDictionary<string, object>`；以編譯好的存取委派讀取，依型別快取 |

- 解析結果（SQL 片段、插槽、參數清單）依 SQL 字串快取；字串常值、`--`／`/* */` 註解與 `[識別項]` 中的 `@`、`{=` 不會被誤判。
- 結果對應用同一套 `ShaperCompiler`：欄位名稱比對屬性名稱與 `[Column(Name)]`，不分大小寫；純量型別（`QueryAsync<int>`）取第一欄。
- 不提供 Dapper 的 multi-mapping（`splitOn`）、`QueryMultiple`、自訂 `TypeHandler`；這些情境改用 LINQ（Join、`Include`）或 5.x 的 Value Converter。

### 5.13 連線、交易與執行

- **連線**：每次操作開一條（交給連線池）；在 `ChefTransaction` 或 `TransactionScope` 內則沿用。
- **ChefTransaction**：`factory.BeginTransactionAsync("MemberDB")` 開啟連線與交易，存在 `AsyncLocal<TransactionContext>`；之後連線字串相同的操作自動加入。巢狀呼叫時沿用外層交易（不支援 savepoint 以外的巢狀交易；`CreateSavepointAsync` 列為 5.x）。
- **TransactionScope**：SqlClient 自動 enlist；Chef 不另外開交易。3.x 判斷 `Transaction.Current` 的邏輯保留。
- **重試**：選用的 `IExecutionStrategy`（參考 EF Core `SqlServerRetryingExecutionStrategy` 的暫時性錯誤清單）；預設不重試，交易中不重試。
- **逾時**：全域 `CommandTimeout`、單一查詢 `.WithCommandTimeout(...)`。
- **同步 API**：`ToList()`、`First()` 等標準 LINQ 同步執行一樣支援（底層使用同步的 ADO.NET 呼叫，不做 sync-over-async）。

### 5.14 診斷與可觀察性

| 機制 | 內容 |
|---|---|
| `ToQueryString()` | 內嵌的值直接出現在 SQL 中；參數輸出成 `DECLARE @prefix_startswith nvarchar(50) = N'J%';` 等宣告，可以直接在 SSMS 執行 |
| `OutputSql`（3.x） | 保留，**一律帶參數** |
| `OnDbError`（3.x） | 保留，查詢與寫入都會呼叫 |
| `ILogger` | 分類 `Chef.DbAccess.Command`、`Chef.DbAccess.Query`；`EnableSensitiveDataLogging` 才記錄參數值 |
| `DiagnosticSource`／`ActivitySource("Chef.DbAccess")` | `CommandExecuting`、`CommandExecuted`（含耗時、筆數）、`CommandError`、`QueryCompiled`（快取未命中）；可以接 OpenTelemetry |
| `TagWith("...")` | 在 SQL 前加上 `-- 註解`，方便在 Profiler／Query Store 對回程式碼 |
| 統計 | `ChefDbAccess.Diagnostics.QueryCacheHits／Misses`，供效能調校 |

### 5.15 擴充點一覽

| 擴充點 | 用途 |
|---|---|
| `IMethodCallTranslator`／`IMemberTranslator`／`IAggregateMethodCallTranslator` | 新增函式對應 |
| `[DbFunction]` | 對應純量函式／資料表值函式（TVF 列為 5.x） |
| `ITypeMappingSource` 裝飾 | 自訂型別對應 |
| `ICommandInterceptor` | 執行前後改寫命令、記錄、加 hint |
| `IDatabaseProvider` | 新的資料庫方言 |
| `IExecutionStrategy` | 重試策略 |

### 5.16 SQL Server 版本基準

**最低支援 SQL Server 2016 SP1**（D14），而且不要求資料庫的相容性層級：從舊版還原上來、仍停在 100／110／120 的資料庫一樣要能用。所以 5.0 產生的 SQL 不使用 2017 以後才有的語法，也不使用需要相容性層級 130 的功能。

| C# | 較新版本的寫法（不使用） | 5.0 產生 |
|---|---|---|
| `Trim()` | `TRIM`（2017） | `LTRIM(RTRIM(x))`；`TrimStart`／`TrimEnd` 只支援空白，帶字元參數時丟出翻譯例外 |
| `Math.Max(a, b)`／`Math.Min(a, b)` | `GREATEST`／`LEAST`（2022） | `CASE WHEN a >= b THEN a ELSE b END` |
| 兩個可為 null 的欄位比較 | `IS [NOT] DISTINCT FROM`（2022） | `OR` 展開（§5.7.4） |
| `DateTime.Date` | `DATETRUNC`（2022） | `CONVERT(date, x)`，必要時再轉回原型別 |
| 字串串接 | `CONCAT_WS`（2017） | `+`；可為 null 的運算元加上 `ISNULL(x, N'')`（C# 中 null 串接視為空字串） |
| 大量 `Contains` | `OPENJSON`、`STRING_SPLIT`（相容性層級 130） | `#暫存資料表`（§5.8） |
| `string.Join` 聚合 | `STRING_AGG`（2017） | 5.0 不支援；5.x 加入時需要明確啟用較新的版本 |

- 方言選項 `UseSqlServer(o => o.TargetVersion(SqlServerVersion.V2016))` 是預設值。5.x 之後若要使用較新的函式（`TRIM`、`STRING_AGG`、`GREATEST`），必須明確調高版本；翻譯器依版本選擇寫法，版本也納入查詢快取鍵。
- **防退化**：SQL 快照測試另外掃描所有產生的 SQL，出現 `TRIM(`、`GREATEST(`、`LEAST(`、`STRING_AGG`、`OPENJSON`、`STRING_SPLIT`、`IS DISTINCT FROM`、`DATETRUNC`、`CONCAT_WS` 等關鍵字就失敗（基準為 V2016 時）。
- **實機驗證**：Linux 的 SQL Server 容器最舊只有 2017，所以 CI 另外用 Windows runner 安裝 SQL Server 2016 SP1 Express，在相容性層級 100 與 130 下各跑一次完整整合測試（Phase 0.6）。

---

## 6. LINQ 支援矩陣

| 類別 | 5.0 | 5.x 以後 |
|---|---|---|
| 篩選與排序 | `Where`、`OrderBy(Descending)`、`ThenBy(Descending)`、`Skip`、`Take`、`Distinct`、`Reverse`（有排序時） | — |
| 投影 | 實體、部分欄位的實體（member-init）、匿名型別、DTO、純量、巢狀物件；`?:`→`CASE`、`??`→`COALESCE`；最外層用戶端求值 | 巢狀集合投影（`Select(x => new { x.Name, Depts = x.ManagedDepartments.ToList() })`） |
| 終結運算子 | `First`、`Single`、`Last`（有排序時）、`ElementAt`（含 `OrDefault`）、`Count`、`LongCount`、`Any`、`All`、`Contains`、`Min`、`Max`、`Sum`、`Average`、`ToList`、`ToArray`、`ToDictionary`、`AsAsyncEnumerable` | — |
| 分組 | `GroupBy`＋聚合＋Having；最終投影為 `IGrouping`（用戶端分組，記錄警告） | `string.Join`／`STRING_AGG` 聚合（需明確調高版本，§5.16） |
| Join | `Join`、`GroupJoin`＋`SelectMany`＋`DefaultIfEmpty`、`LeftJoin`、`CROSS JOIN`、`CROSS／OUTER APPLY`、導覽屬性、`Include`／`ThenInclude`、filtered include、跨資料庫 | `RightJoin`、`AsSplitQuery` |
| 子查詢 | `EXISTS`、`IN`、純量子查詢、相關子查詢、閉包中的 `IQueryable` | — |
| 集合運算 | `Union`、`Concat`、`Intersect`、`Except`；集合 `Contains`（依型別與數量選擇策略，§5.8） | — |
| 字串 | `Contains`、`StartsWith`、`EndsWith`（皆跳脫萬用字元）、`IndexOf`、`Replace`、`Substring`、`ToUpper`、`ToLower`、`Trim`／`TrimStart`／`TrimEnd`（`LTRIM`／`RTRIM`，§5.16）、`Length`、`IsNullOrEmpty`、`IsNullOrWhiteSpace`、`+`／`Concat`、`CompareTo`、`string.Compare`、`ChefFunctions.Like` | `Split` 相關、正規表示式 |
| 日期時間 | `DateTime`／`DateTimeOffset`／`DateOnly`／`TimeOnly` 的 `Year`…`Millisecond`、`Date`、`DayOfWeek`、`DayOfYear`、`Add*`、`Now`／`UtcNow`／`Today`（用戶端求值成參數；伺服器端用 `ChefFunctions.ServerNow()` 等，見 D11）、`ChefFunctions.DateDiffDay` 等 | `AT TIME ZONE` |
| 數學 | `Math.Abs`、`Ceiling`、`Floor`、`Round`、`Truncate`、`Pow`、`Sqrt`、`Sign`、`Max`／`Min`（`CASE WHEN`，§5.16） | 三角函式 |
| 轉型 | `(int)x`、`Convert.ToXxx`、`ToString()`、列舉、`Nullable<T>.HasValue`／`Value`／`GetValueOrDefault` | Value Converter |
| SQL Server 專屬 | 全文檢索 `Includes()`（`CONTAINS`）、`FreeText()`、`WithNoLock()`／`WithoutNoLock()`／`WithHint()`、`WithOption("RECOMPILE")`、`TagWith`、三段式名稱 | Temporal Table、JSON 欄位、`STRING_AGG`、視窗函式 |
| 寫入 | `ExecuteUpdate`、`ExecuteDelete`（含 JOIN、`OUTPUT`）、`Insert`（單筆、setter、多筆、`whenNotExists`）、`Upsert`、`BulkInsert`／`BulkUpdate`／`BulkUpsert`／`BulkDelete` | — |
| Raw SQL | 可組合的 `FromSql`（插值依 §5.7 內嵌或參數化）、`QueryAsync<T>(sql, param)`、`ExecuteAsync(sql, param)`，支援 `@name`、`{=name}`、`IN @list` | `QueryMultiple` |

---

## 7. 3.x → 5.0 升級

### 7.1 原則

- **不遷就舊版**：3.x 的 `QueryObject`、`Chef.DbAccess.Fluent`、`Grouping<T…>`、多載爆炸的 `IDataAccess<T>` 方法全部移除，不提供相容套件、不標 `[Obsolete]` 過渡。要升級的專案依 §7.2 改寫程式碼。
- **錯誤要在編譯期出現**：舊 API 是直接消失而不是行為改變，升級時編譯器會指出每一個要改的地方；刻意避免「名稱相同、語意不同」的 API，讓編譯通過的程式碼不會悄悄換了行為。
- **保留的語意就完全保留**：`[Required]` 決定 Insert 欄位（D3）、數值與布林值內嵌（D5）、預設 `NOLOCK`（D4）、`[ConnectionString]`、`OutputSql`／`OnDbError`、Raw SQL 的 `@name`／`{=name}` 寫法，這些在 5.0 不需要改。

### 7.2 破壞性變更對照

| 3.x | 5.0 |
|---|---|
| `Where(...).Select(...).QueryAsync()` | `Where(...).Select(...).ToListAsync()` |
| `QueryOneAsync()` | `SingleOrDefaultAsync()`（3.x 超過一筆會丟例外，語意相同） |
| `ExistsAsync()` | `AnyAsync()` |
| `Where(a).Where(b)` 時第一個條件被覆蓋 | 兩個條件以 `AND` 結合（標準 LINQ 語意）；依賴舊行為的地方要檢查 |
| `And(...)`／`Or(...)` | 寫在同一個 Lambda 裡（`&&`、`||`），或分段 `query = query.Where(...)` |
| 必須 `Select` | 不 `Select` 時選出全部對應欄位 |
| `Select(x => new { x.Name })` 回傳部分填值的 `T` | `Select(x => new Member { Name = x.Name })`；`Select(x => new { x.Name })` 回傳匿名型別 |
| `QueryAsync(x => new {...})`（用戶端轉型） | `Select(...)` 在資料庫端投影 |
| `InnerJoin`／`LeftJoin(nav, condition)` | `Include(nav[, condition])`、`Join`、`LeftJoin`、導覽屬性 |
| `GroupBy(columns, g => new T { ... g.Avg(...) })` | 標準 `GroupBy(...).Select(g => ...)` |
| `Set(...).UpdateAsync()` | `ExecuteUpdateAsync(x => new T { ... })` |
| `DeleteAsync(predicate)` | `Where(predicate).ExecuteDeleteAsync()` |
| `UpsertAsync(predicate, setter)` | `UpsertAsync(entity/setter, on:)` |
| `BulkInsertAsync`／`BulkUpdateAsync` 等舊簽章 | §2.6 的新簽章（`on:`、`columns:`、`returning:`）；`BulkInsertAsync(setterTemplate, values)` 改成 `BulkInsertAsync(values, columns: x => new { ... })` |
| 一對多合併靠 `Equals`／`GetHashCode` | 靠主鍵：加上 `[Key]` 或命名為 `Id` |
| `IsDirtyRead` 屬性 | `Options.DirtyRead`、`.WithNoLock()`／`.WithoutNoLock()` |
| `ArgumentException`（翻譯失敗） | `ChefQueryTranslationException : NotSupportedException` |
| 自行實作 `IDataAccess<T>` | 介面完全不同，改繼承新的基底類別 |
| 沒有 `CancellationToken` | 全部非同步 API 都有 |
| `[ConnectionString(name, Schema)]` | `[ConnectionString(name)]`＋`[Table(Schema = ...)]` |
| 底層是 Dapper | 不再相依 Dapper：Raw SQL 寫法相同，但不支援 multi-mapping、`QueryMultiple`、`TypeHandler`；專案中直接使用 Dapper 的程式碼要自行加入 Dapper 套件參考 |
| Bulk 需要 `CREATE TYPE` 權限 | 不需要（§5.12.3），可以收回該權限 |

### 7.3 升級步驟

1. 升級套件，依編譯錯誤與 §7.2 改寫。
2. 有 `Include` 一對多的模型補上 `[Key]`（或命名為 `Id`）。
3. 對重要查詢用 `ToQueryString()` 檢查產生的 SQL。
4. 跑整合測試；Phase 0 錄下的 3.x 資料快照可以作為「改寫後結果一致」的比對基準。

---

## 8. 實施階段

每個階段結束都要能出 preview 套件，讓團隊實際試用；每個工作項目都以「測試先行、小 commit」進行。工作規模以 S（1～2 天）、M（3～5 天）、L（1～2 週）粗估。

### Phase 0：工程整頓

| # | 工作 | 規模 |
|---|---|---|
| 0.1 | `Directory.Build.props`（`LangVersion`、`Nullable`、`TreatWarningsAsErrors`、版本號 `5.0.0-alpha`）、`Directory.Packages.props`（Central Package Management） | S |
| 0.2 | `Chef.DbAccess.SqlServer` 改成 ProjectReference；函式庫只以 `netstandard2.0` 為目標（D6）。Dapper 套件參考此時**保留**：3.x 程式碼建立在 Dapper 上，0.4 的快照與 0.8 的基準數字都需要 3.x 能執行；移除時機改到 6.6 | S |
| 0.3 | 測試專案改成 SDK-style（MSTest v3，D9）、拆成 `Chef.DbAccess.Tests`（純單元）與 `Chef.DbAccess.SqlServer.Tests`（快照＋整合） | M |
| 0.4 | **3.x 特徵化測試**：把各種 API 產生的 SQL 與參數錄成快照（不需要資料庫），再把整合測試結果錄成「資料快照」。5.0 不相容 3.x API，這些快照的用途是：同一個情境用 5.0 改寫後，**資料結果**要一致（SQL 文字可以不同） | M |
| 0.5 | 整合測試基礎設施：Windows 開發機用 LocalDB（沿用 `test/SampleDB.sql`），CI 用 Testcontainers（`mcr.microsoft.com/mssql/server`，最舊 2017）；以 `[TestCategory("Integration")]` 區隔 | M |
| 0.6 | GitHub Actions：build、單元＋快照測試、整合測試（Linux＋容器）、**SQL Server 2016 SP1 相容性測試**（Windows runner＋2016 SP1 Express，相容性層級 100 與 130，§5.16）、`dotnet pack` | M |
| 0.7 | 測試與範例專案的目標設為 `net10.0;net48`（同時驗證 .NET 與 .NET Framework），改成 ProjectReference | S |
| 0.8 | `Chef.DbAccess.Benchmarks`（BenchmarkDotNet）：先錄下 3.x 與 Dapper 在 §10.2 各情境的基準數字（.NET 10 與 .NET Framework 4.8）；CI 加入配置數量比對 | M |

**交付**：可以在 CI 上綠燈建置；3.x 行為被快照鎖住。

### Phase 1：Metadata、型別對應與 DataSource

| # | 工作 | EF Core 參考 | 規模 |
|---|---|---|---|
| 1.1 | `EntityType`／`Property`／`Key`：解析 DataAnnotations，`EntityTypeCache<T>` | `efcore/src/EFCore/Metadata/Conventions/*AttributeConvention.cs` | M |
| 1.2 | 慣例：主鍵（`Id`／`<類別>Id`）、Identity 推斷、`[DatabaseGenerated]` | 同上 | S |
| 1.3 | `Navigation`／`ForeignKey`：`[ForeignKey]`、`[InverseProperty]`、命名慣例 | `ForeignKeyAttributeConvention.cs`、`InversePropertyAttributeConvention.cs` | M |
| 1.4 | 全域 Fluent 設定 `ChefDbAccess.Entity<T>(...)`，第一次使用後凍結 | — | M |
| 1.5 | `TypeMapping`、`TypeMappingSource`（核心）＋`SqlServerTypeMappingSource` | `efcore/src/EFCore.SqlServer/Storage/Internal/SqlServerTypeMappingSource.cs` | M |
| 1.6 | `DataSource`：以 `DbConnectionStringBuilder` 解析伺服器與資料庫（取代 Regex）、`[ConnectionString]` 解析、`Create<T>(tableName)` 覆寫 | — | S |

**交付**：Metadata 與型別對應有完整單元測試（含 3.x 範例模型）。

### Phase 2：SQL AST 與 SQL 產生器

| # | 工作 | EF Core 參考 | 規模 |
|---|---|---|---|
| 2.1 | 純量節點、`SelectExpression`、來源節點（Table、Join、Apply、集合運算、FromSql） | `efcore/src/EFCore.Relational/Query/SqlExpressions/` | L |
| 2.2 | `SqlExpressionFactory`：建立節點並推導 `TypeMapping` | `SqlExpressionFactory.cs` | M |
| 2.3 | `QuerySqlGenerator`（ANSI）：識別項跳脫、運算子優先順序與括號、別名產生 | `QuerySqlGenerator.cs`、`SqlAliasManager.cs` | M |
| 2.4 | `SqlServerQuerySqlGenerator`：`TOP`、`OFFSET／FETCH`、table hint、`OPTION`、三段式名稱、`TagWith` 註解 | `SqlServerQuerySqlGenerator.cs` | M |
| 2.5 | `ChefCommand`（SQL 片段＋字面值插槽＋參數繫結描述，§5.7.2）、字面值格式化器、`ToQueryString` 的 `DECLARE` 輸出 | `SqlServerQueryStringFactory.cs` | M |

**交付**：可以手動組 AST 並產生正確 T-SQL，快照測試涵蓋所有節點。

### Phase 3：IQueryable MVP（單表）→ **5.0.0-preview1**

| # | 工作 | EF Core 參考 | 規模 |
|---|---|---|---|
| 3.1 | `DataAccessQueryRootExpression`、`ChefQueryable<T>`、`ChefQueryProvider`、`IAsyncQueryProvider`；`SqlServerDataAccess<T>` 實作 `IOrderedQueryable<T>` | `EntityQueryable\`.cs`、`EntityQueryProvider.cs`、`EntityQueryRootExpression.cs` | M |
| 3.2 | `ParameterExtractor`（閉包、可求值子樹、依型別標記內嵌或參數化、`ChefFunctions.Constant`／`Parameter`、`DateTime.Now` 用戶端求值與 `ChefFunctions.Server*`、閉包中的 `IQueryable` 內嵌、C# 14 的 `MemoryExtensions.Contains`／`SequenceEqual` 改寫回 `Enumerable`，§5.8、D15） | `ExpressionTreeFuncletizer.cs` | L |
| 3.3 | `ExpressionShapeComparer`、`QueryCacheKey`、有上限的 `QueryCache` | `ExpressionEqualityComparer.cs`、`CompiledQueryCacheKeyGenerator.cs` | M |
| 3.4 | `SqlTranslator`：比較、邏輯、算術、位元、null、布林欄位、列舉、`Nullable`、`Equals`、`?:`、`??`、轉型 | `RelationalSqlTranslatingExpressionVisitor.cs` | L |
| 3.5 | 翻譯器：字串、日期時間、數學、轉型、全文檢索（SQL Server） | `efcore/src/EFCore.SqlServer/Query/Internal/Translators/` | L |
| 3.6 | `QueryableMethodTranslator`（單表）：`Where`、`OrderBy`／`ThenBy`、`Skip`／`Take`、`Distinct`、`Select`、所有終結運算子與純量聚合 | `RelationalQueryableMethodTranslatingExpressionVisitor.cs` | L |
| 3.7 | `NullSemanticsProcessor`、`ParameterBasedSqlProcessor`（null 值、集合 `Contains` 策略，§5.8）、`CommandCache`、字面值插槽填值 | `SqlNullabilityProcessor.cs`、`RelationalParameterBasedSqlProcessor.cs`、`Internal/RelationalCommandCache.cs` | L |
| 3.8 | `ShaperCompiler`：實體、部分實體、匿名型別、DTO、純量、巢狀物件、最外層用戶端求值；強型別讀值（不裝箱） | `RelationalShapedQueryCompilingExpressionVisitor.ShaperProcessingExpressionVisitor.cs` | L |
| 3.9 | 執行：`QueryingEnumerable<T>`（同步／非同步串流）、連線管理、`OutputSql`／`OnDbError`／`ILogger`／`DiagnosticSource` | `Internal/SingleQueryingEnumerable.cs` | M |
| 3.10 | 非同步運算子（全部帶 `CancellationToken`）、`ToQueryString`、`WithNoLock`／`WithoutNoLock`／`WithHint`／`TagWith`／`WithCommandTimeout` | `EntityFrameworkQueryableExtensions.cs` | M |
| 3.11 | `ChefQueryTranslationException`＋`ExpressionPrinter` | `ExpressionPrinter.cs` | S |
| 3.12 | `QueryAsserter` 測試框架：同一查詢對資料庫與記憶體資料各跑一次並比對 | `efcore/test/EFCore.Specification.Tests/TestUtilities/QueryAsserter.cs` | M |

**交付**：單表查詢全面可用；`members.Where(...).Select(...).ToListAsync()` 上線；**通過 §10.2「物件產生」與「LINQ 主鍵查詢」的放行條件**後，發 preview1 給團隊試用。

### Phase 4：Join 與導覽屬性 → **5.0.0-preview2**

| # | 工作 | EF Core 參考 | 規模 |
|---|---|---|---|
| 4.1 | `QueryNormalizer`：查詢語法、透明識別項、GroupJoin＋SelectMany＋DefaultIfEmpty → LeftJoin；以執行期偵測辨識 .NET 10 的 `Queryable.LeftJoin`，並在 `Chef.DbAccess.Linq` 提供給舊執行環境的 `LeftJoin` | `QueryableMethodNormalizingExpressionVisitor.cs` | M |
| 4.2 | `Join`、`LeftJoin`、`CROSS JOIN`、相關 `SelectMany` → `CROSS／OUTER APPLY`（能化簡就化簡），複合鍵 | `RelationalQueryableMethodTranslatingExpressionVisitor.cs`（TranslateJoin／TranslateSelectMany） | L |
| 4.3 | `NavigationExpander`：參考導覽 → JOIN（依外鍵可否為 null 決定 INNER／LEFT）、集合導覽 → EXISTS／純量子查詢 | `NavigationExpandingExpressionVisitor*.cs` | L |
| 4.4 | `Include`／`ThenInclude`／filtered include／3.x 條件式 `Include`；`CollectionShaper` identity resolution；分頁時先子查詢 | `IncludeExpression.cs`、`Internal/SingleQueryCollectionContext.cs` | L |
| 4.5 | 跨資料庫三段式名稱、跨伺服器偵測；快取鍵納入 `DataSource` | — | S |
| 4.6 | 整合測試：3.x 範例的所有 Join 情境、跨資料庫 | — | M |

### Phase 5：子查詢、GroupBy、集合運算 → **5.0.0-preview3**

| # | 工作 | EF Core 參考 | 規模 |
|---|---|---|---|
| 5.1 | Pushdown 規則（§5.11）、子查詢 `ORDER BY` 清理、`SqlTreePruner` | `SelectExpression.cs`（PushdownIntoSubquery）、`SqlTreePruner.cs` | L |
| 5.2 | `EXISTS`／`IN`／純量子查詢、相關子查詢 | `ExistsExpression.cs`、`ScalarSubqueryExpression.cs` | M |
| 5.3 | `GroupBy`＋聚合＋`HAVING`、以匿名型別分組、分組後投影 | `RelationalGroupByShaperExpression.cs` | L |
| 5.4 | `Union`、`Concat`、`Intersect`、`Except` | `SetOperationBase.cs` | M |
| 5.5 | 大量 `Contains` 的 `#暫存資料表` 模式（與 Phase 6.4 共用 `SqlBulkCopy` 機制，可先做最小版本）；`TargetVersion` 選項與 §5.16 的防退化掃描 | — | M |

### Phase 6：寫入 → **5.0.0-preview4**

| # | 工作 | EF Core 參考 | 規模 |
|---|---|---|---|
| 6.1 | `ExecuteUpdateAsync`（member-init 與 `SetProperty` 兩種 setter）、`ExecuteDeleteAsync`、JOIN、`returning:` | `RelationalQueryableMethodTranslatingExpressionVisitor.ExecuteUpdate.cs`／`.ExecuteDelete.cs`、`UpdateSettersBuilder.cs` | L |
| 6.2 | `InsertAsync`：單筆（`[Required]` 欄位）、setter、多筆自動分批、`whenNotExists`、`returning:`、`[HasTrigger]` | `efcore/src/EFCore.SqlServer/Update/Internal/SqlServerUpdateSqlGenerator.cs` | M |
| 6.3 | `UpsertAsync`：UPDLOCK＋SERIALIZABLE 策略與 MERGE 策略 | 同上（MERGE 產生方式） | M |
| 6.4 | Bulk：`EntityDataReader<T>`、`SqlBulkCopy`、`#暫存資料表`流程（定序、清理、交易）、`BulkInsert／Update／Upsert／Delete` | — | L |
| 6.5 | `ChefTransaction`（AsyncLocal）、`TransactionScope` 相容、`IExecutionStrategy` | `efcore/src/EFCore.SqlServer/Storage/Internal/SqlServerTransientExceptionDetector.cs` | M |
| 6.6 | `FromSql`（可組合、插值依 §5.7 處理）；`RawSqlParser`（`@name`、`{=name}`、`IN @list`）與 `QueryAsync<T>`／`ExecuteAsync`，取代 Dapper（§5.12.4）；移除 `Chef.DbAccess.SqlServer` 的 Dapper 套件參考，連同仍依賴 Dapper 的 3.x 程式碼一起刪除（D7） | `FromSqlExpression.cs`、`Internal/FromSqlQueryRootExpression.cs` | M |
| 6.7 | 跑 §10.2 Raw SQL 與寫入的放行條件，通過後才發 preview4 | S |

### Phase 7：文件、範例與試升級 → **5.0.0-rc1**

| # | 工作 | 規模 |
|---|---|---|
| 7.1 | 把 Phase 0 的 3.x 情境逐一用 5.0 改寫，比對資料快照，修正差異；整理成升級對照表（§7.2）的範例 | M |
| 7.2 | 移除 3.x 原始碼（`QueryObject`、`Fluent/`、`Grouping`、`StatementExtension`、`SqlBuilder` 等） | S |
| 7.3 | 升級指南（§7）、`doc/` 下的文件改寫為 5.0 版本、範例專案改寫 | M |
| 7.4 | 在一個實際專案試升級，收集回饋 | M |

### Phase 8：效能與正式版 → **5.0.0**

| # | 工作 | 規模 |
|---|---|---|
| 8.1 | 跑完整的 §10.2 放行條件（加上 EF Core `AsNoTracking` 作為參考）；Raw SQL 路徑仍未達標時啟用 §10.3 的退路 | M |
| 8.2 | 依結果調校：快取查找、Shaper、參數建立、配置數量 | M |
| 8.3 | `ChefQuery.Compile`／`CompileAsync`，並通過「LINQ 預先編譯」的放行條件 | S |
| 8.4 | API 審查（公開介面凍結）、XML 文件註解、套件中繼資料、Source Link | S |

### 5.x 之後的候選

`AsSplitQuery`、巢狀集合投影、Value Converter、視窗函式、`STRING_AGG`、Temporal Table、JSON 欄位、TVF、SQLite 與 PostgreSQL 方言、Roslyn Analyzer（在編譯期提示無法翻譯的運算式）、Source Generator 產生 Shaper／預先編譯查詢、Raw SQL 的 `QueryMultiple`。

---

## 9. 測試策略

| 層級 | 內容 | 需要資料庫 | 位置 |
|---|---|---|---|
| 單元測試 | Metadata、型別對應、`ParameterExtractor`、快取鍵、AST、SQL 產生器、Null 語意、Shaper（以假的 `DbDataReader`） | 否 | `Chef.DbAccess.Tests` |
| SQL 快照測試 | 每個 LINQ 情境斷言產生的 SQL 與參數（`AssertSql(...)`，比照 EF Core 的 `*SqlServerTest.cs`） | 否 | `Chef.DbAccess.SqlServer.Tests/Query` |
| 功能測試 | `QueryAsserter`：同一個查詢對 SQL Server 與記憶體資料各執行一次並比對結果，確保語意與 LINQ to Objects 一致（null、排序、分組） | 是 | `Chef.DbAccess.SqlServer.Tests/Functional` |
| 3.x 特徵化 | Phase 0 錄下的 SQL 快照與資料快照，給 Phase 7 改寫後比對資料結果 | 部分 | `Chef.DbAccess.SqlServer.Tests/Characterization` |
| 效能基準 | §10.2 放行條件；CI 每個 PR 比對配置數量，每晚在固定機器量測時間 | 是 | `Chef.DbAccess.Benchmarks` |
| SQL Server 2016 SP1 相容性 | 產生的 SQL 關鍵字掃描（不需要資料庫）＋2016 SP1 實機整合測試（相容性層級 100、130） | 部分 | `Chef.DbAccess.SqlServer.Tests` |

- 測試資料：以 `test/SampleDB.sql` 為基礎擴充（會員、部門、多對一、一對多、可為 null 的外鍵、`varchar` 欄位、全文檢索索引、跨資料庫）。
- 涵蓋率目標：核心 85％ 以上；每個翻譯器至少一個快照＋一個功能測試。
- 從 EF Core 的 `NorthwindWhereQueryTestBase.cs` 等規格測試挑選情境改寫（只取情境，不複製程式碼）。
- C# 版本：v5 的測試以 C# 14 編譯，並在 `net10.0` 上執行，才能涵蓋 first-class span 產生的運算式樹（D15）。`Chef.DbAccess.SqlServer.Tests` 目前為了 3.x 特徵化測試暫時固定 `LangVersion` 13，v5 的測試開始加入時移除。

---

## 10. 效能：不比 Dapper 慢

3.x 相依 Dapper 是為了效能，所以移除 Dapper（D7）的前提是**不掉效能**。這不能只靠口頭保證，5.0 的做法是：設計上消除和 Dapper 的差距、把「不慢於 Dapper」寫成每個階段的放行條件，並準備達不到時的退路。

### 10.1 成本拆解與設計

一次查詢的成本可以拆成四塊，逐塊對照 Dapper：

| 成本 | Dapper 怎麼做 | Chef 5.0 怎麼做 | 預期 |
|---|---|---|---|
| ① 讀取資料列並產生物件 | 依「SQL＋型別＋結果欄位結構」快取一份 IL 產生的反序列化器；第一次遇到某個結果結構時要比對欄位名稱，之後以索引子 `reader[i]` 取得 `object` 再拆箱 | LINQ 路徑在編譯查詢時就知道每一欄的順序與型別，Shaper 直接用序號與強型別方法（`GetInt32`、`GetString`）讀值，不比對名稱、不裝箱；以 `CommandBehavior.SequentialAccess` 依序讀取。Raw SQL 路徑和 Dapper 一樣依結果欄位結構快取 Shaper，但同樣使用強型別讀值 | **不慢於 Dapper，配置更少** |
| ② 建立命令與參數 | IL 產生的參數讀取器，`SqlParameter` 的型別由值推斷 | 內嵌的值不需要 `SqlParameter`；需要參數時直接指定 `SqlDbType` 與長度，不讓 SqlClient 推斷 | **不慢於 Dapper** |
| ③ SQL 字串 | 使用者直接給 | LINQ 路徑：快取的範本＋插槽填值（§5.7.2）；Raw SQL 路徑：依 SQL 字串快取解析結果 | 相當 |
| ④ 運算式樹處理（走訪、計算形狀雜湊、抽出值） | **沒有這一層** | 一次走訪同時完成雜湊與抽值；閉包欄位以快取的存取委派讀取，不在每次查詢時呼叫 `Expression.Compile()`；`QueryContext` 重複使用，避免配置 | **唯一一定比 Dapper 多的成本**，目標是單表主鍵查詢 ≤ 2 µs。本機 SQL Server 一趟往返通常超過 100 µs，所以約占 2％ 以內；要完全消除時用 `ChefQuery.Compile`，跳過整個④ |

其他：

- 3.x 每次呼叫都重新解析運算式、重新組 SQL 字串，然後才交給 Dapper，所以 5.0 的 LINQ 路徑在所有情境都應該比 3.x 快。
- 3.x 為了 Dapper 快取而有的 SqlException 137／102 清快取重試（`IDbConnectionExtension.cs:26`）也一併消失。
- 以 `Expression.Compile()` 產生的委派和 Dapper 的 IL 一樣是 `DynamicMethod`，在 .NET 與 .NET Framework 上都會經過 JIT，執行速度屬於同一個等級。

### 10.2 放行條件

以 BenchmarkDotNet（`MemoryDiagnoser`）在同一台機器、本機 SQL Server 上量測，**.NET 10 與 .NET Framework 4.8 兩種執行環境都要通過**。

| 情境 | 比較對象 | 條件 |
|---|---|---|
| 物件產生：假的 `DbDataReader`，1 萬筆 × 10 欄（排除網路雜訊） | Dapper | 時間 ≤ Dapper；配置 ≤ Dapper |
| Raw SQL `QueryAsync<T>`：主鍵查一筆、查 1,000 筆、1 萬筆 × 10 欄 | Dapper（同一句 SQL） | 時間 ≤ Dapper × 1.03（3％ 為量測誤差容許）；配置 ≤ Dapper |
| Raw SQL `ExecuteAsync`：單筆 UPDATE | Dapper | 同上 |
| LINQ 預先編譯（`ChefQuery.Compile`）：主鍵查一筆 | Dapper 手寫等價 SQL | 時間 ≤ Dapper × 1.03；配置 ≤ Dapper |
| LINQ 一般寫法（快取命中）：主鍵查一筆、條件＋分頁、三表 JOIN | Dapper 手寫等價 SQL | 額外時間 ≤ 2 µs；額外配置 ≤ 1 KB |
| 所有情境 | 3.x | 不慢於 3.x |
| BulkInsert 10 萬筆 | 3.x TVP | 不慢於 3.x，且不需要 DDL 權限 |
| 第一次翻譯（快取未命中，單表條件＋投影） | — | ≤ 1 ms |
| 字面值填入插槽（10 個插槽） | — | ≤ 1 µs、一次字串配置 |

完整量測情境：主鍵查詢、條件＋分頁、三表 JOIN＋投影、一對多 Include、GroupBy、`Contains` 10／1,000／10,000 筆、ExecuteUpdate、Insert、BulkInsert、Raw SQL。另外附上 EF Core（`AsNoTracking`）作為參考，不列入放行條件。

### 10.3 怎麼確保做得到

- **從 Phase 0 開始量**：基準專案與 3.x、Dapper 的基準數字在 Phase 0 就建立（0.8），不是最後才補。
- **每個 preview 都要過關**：Phase 3（preview1）必須通過「物件產生」與「LINQ 主鍵查詢」的條件；Phase 6（preview4）必須通過 Raw SQL 與寫入的條件。沒過關就不發 preview，先調校。
- **CI 防退化**：配置數量是固定值，每個 PR 都比對，增加就失敗；執行時間在固定機器上每晚量測，退步超過 5％ 時標記。
- **退路**：如果 Phase 8 結束時 Raw SQL 路徑仍然達不到條件，5.0 的 Raw SQL API 改成內部委派給 Dapper（`Chef.DbAccess.SqlServer` 重新相依 Dapper，API 與 `{=}` 語法不變），由專案負責人決定是否採用。所以最差的情況是和 Dapper 一樣快，不會更慢。LINQ 路徑沒有 Dapper 可以委派，對應的退路是 `ChefQuery.Compile`。

---

## 11. 風險與對策

| 風險 | 影響 | 對策 |
|---|---|---|
| 自寫 LINQ Provider 工作量大、邊界情況多 | 時程延誤 | 分階段、每階段可出 preview；嚴格參考 EF Core 的分層與命名，只做需要的子集；`QueryAsserter` 早期導入 |
| 原本在編譯期就失敗的寫法變成執行期才失敗 | 使用者困擾 | 明確的例外訊息＋`ExpressionPrinter`；`ToQueryString()`；5.x 的 Roslyn Analyzer |
| 升級必須改程式碼 | 既有專案升級成本 | 舊 API 直接移除，所有要改的地方都是編譯錯誤；§7.2 對照表；保留的語意（`[Required]`、內嵌、`NOLOCK`、Raw SQL 語法）完全不變 |
| 字面值內嵌讓執行計畫數量增加 | SQL Server 計畫快取的記憶體 | 這是避免參數探查的取捨（§5.7.2）；文件建議 `optimize for ad hoc workloads`；可以全域或單值改回參數 |
| 移除 Dapper 後效能退步 | 違反 D7 的前提 | §10：逐塊對照 Dapper 的設計、每個 preview 的放行條件、CI 防退化；Raw SQL 路徑保留委派回 Dapper 的退路 |
| SQL Server 2016 SP1 缺少新語法 | 部分 LINQ 寫法的翻譯較冗長 | §5.16 的替代寫法；新語法要明確調高 `TargetVersion` 才使用；關鍵字掃描防止誤用 |
| 查詢快取無限成長（動態組出大量形狀） | 記憶體 | 有上限的 LRU、快取統計；值（包括內嵌的值）不進快取鍵，所以內嵌不會讓查詢快取變多 |
| `NOLOCK` 預設造成髒讀、重複或遺漏資料列 | 資料正確性 | 可設定預設值（D4）、單一查詢可關閉、文件明確說明 |
| `IQueryable` 介面讓 `foreach (var m in members)` 意外載入整張表 | 效能事故 | 文件說明；記錄「沒有條件的全表查詢」警告事件 |
| netstandard2.0 缺少新 API（`IAsyncEnumerable`、`LeftJoin`、`DateOnly`） | 功能差異 | `Microsoft.Bcl.AsyncInterfaces`；`LeftJoin`、`DateOnly`／`TimeOnly` 以執行期偵測支援（§5.1） |
| `Microsoft.Data.SqlClient` 日後不再提供 `netstandard2.0` 組件（6.0 曾短暫拿掉，6.1 又加回） | 無法更新相依版本 | 只用穩定的核心 API，應用程式可以自行參考較新版本；屆時再評估是否增加目標框架 |

---

## 12. 決策紀錄

2026-11-01 定案。

| # | 決策 | 結論 | 說明 |
|---|---|---|---|
| D1 | 查詢入口 | `IDataAccess<T>` 直接實作 `IQueryable<T>` | 最接近「拿類別就能 ORM」的目標，等同 EF Core 的 `DbSet<T>` |
| D2 | 3.x API 的去留 | **直接移除** | 不提供相容套件；升級時改寫程式碼（§7） |
| D3 | `InsertAsync(entity)` 寫入哪些欄位 | **維持 3.x 的 `[Required]` 語意** | Chef 以類別為主，Data Model 不必和資料表一對一綁死；`[Required]` 只有這個用途，不影響查詢的 null 語意（§5.5） |
| D4 | `NOLOCK` 預設值 | 維持 3.x 預設加上，改成全域可設定 | `ChefDbAccessOptions.DefaultDirtyRead`，單一查詢用 `.WithoutNoLock()`；風險在文件中說明 |
| D5 | 值的內嵌 | **延續 3.x：`bool` 與所有數值型別（含 `Nullable`）以字面值內嵌** | 避免參數探查；以「SQL 範本＋插槽」實作，翻譯仍只做一次（§5.7）。整數列舉一併內嵌；**值為 null 時不內嵌**，依一般 null 值處理（§5.7.3）；`ChefFunctions.Parameter` 與全域選項可以改回參數 |
| D6 | 目標框架 | **只有 `netstandard2.0`** | 沒有停止支援日期，任何現在與未來的 .NET 都能用；新 API 以執行期偵測支援（§5.1） |
| D7 | Dapper | **完全移除，前提是不慢於 Dapper** | Raw SQL 由 Chef 自己實作，支援 `@name`、`{=name}`、`IN @list`（§5.12.4）；效能以 §10.2 的放行條件把關，Raw SQL 路徑達不到時委派回 Dapper（§10.3） |
| D8 | Null 語意 | C# 語意（預設），可關閉 | `UseRelationalNulls = true` 時輸出 SQL 原生語意 |
| D9 | 測試框架 | 延續 MSTest（v3） | 沿用現有測試；EF Core 的測試情境只取概念 |
| D10 | Upsert 預設策略 | `UPDATE WITH (UPDLOCK, SERIALIZABLE)`＋`INSERT`（交易中） | 語意與 3.x 相同、避開 MERGE 的已知問題；`MERGE WITH (HOLDLOCK)` 作為選項 |
| D11 | `DateTime.Now` 等在哪裡求值 | **用戶端求值成參數** | 伺服器端求值用 `ChefFunctions.ServerNow()`／`ServerUtcNow()`／`ServerToday()`／`ServerSysDateTime()`，或全域 `DateTimeNowEvaluation = Server`（§5.7.1） |
| D12 | SQLite 方言時程 | 5.x | 5.0 聚焦 SQL Server；抽象設計以單元測試保證 |
| D13 | 工廠設定 | 保留 `SqlServerDataAccessFactory.Instance`，另外可以 `new SqlServerDataAccessFactory(options)` | 測試與多租戶情境需要獨立實例 |
| D14 | SQL Server 版本基準 | **SQL Server 2016 SP1，任何資料庫相容性層級** | 不使用 `OPENJSON`、`STRING_SPLIT`（需要相容性層級 130）與 2017 之後的語法；大量 `Contains` 改用 `#暫存資料表`（§5.8、§5.16） |
| D15 | C# 14 first-class span（2026-11-02） | **v5 的 `Contains` 支援 C# 14；3.x 不修正** | `MemoryExtensions.Contains`／`SequenceEqual` 在參數抽取階段改寫回 `Enumerable`（§5.8）；v5 的測試以 C# 14 編譯（§9） |

---

## 附錄 A：3.x 原始碼對照

| 3.x 檔案 | 5.0 去向 |
|---|---|
| `Chef.DbAccess/IDataAccess.cs` | 重寫為小介面（§5.4.1）；舊方法移除 |
| `Chef.DbAccess/QueryObject.cs`、`Fluent/Extension.cs`、`Grouping.cs` | 移除（D2） |
| `Chef.DbAccess/Syntax/StringExtension.cs`（`Includes`） | 保留，由 SQL Server 全文檢索翻譯器處理 |
| `Chef.DbAccess/ConnectionStringAttribute.cs` | 保留；`Schema` 標過時 |
| `SqlServerDataAccess*.cs` | 拆成查詢根（`SqlServerDataAccess<T>`）、執行器、寫入命令建構；`Generate*Statement` 全部由 AST 取代 |
| `Extensions/StatementExtension.cs` | 由 `SqlTranslator`＋翻譯器＋`QuerySqlGenerator` 取代；`MapSqlType`／`CreateSqlMetaData` 移入 `SqlServerTypeMappingSource` |
| `Extensions/IDbConnectionExtension.cs`（Dapper 快取重試） | 移除（不再相依 Dapper） |
| `SqlBuilder.cs`、`UserDefinedColumn.cs` | 移除（AST、`SqlBulkCopy` 取代） |
| `SqlServerDataAccessFactory.cs` | 保留並擴充（D13） |

## 附錄 B：術語

| 術語 | 說明 |
|---|---|
| 查詢根（Query Root） | LINQ 運算式樹最底層代表資料表的節點，Chef 中就是 `IDataAccess<T>` |
| Funcletizer／參數抽取 | 把運算式中不依賴 Lambda 參數、可在用戶端算出的子樹先求值並轉成參數 |
| SQL AST | 與資料庫無關、以物件表示的 SQL 結構（`SelectExpression` 等） |
| Shaper | 描述如何把一列 `DbDataReader` 資料組成結果物件的運算式，最後編譯成委派 |
| Pushdown | 把目前的查詢包成子查詢，以便在外層套用新的運算子 |
| Identity Resolution | 一對多 JOIN 時依主鍵把重複的父資料合併成同一個物件 |
| Null 語意補償 | 在 SQL 中補上 null 判斷，讓比較結果與 C# 一致 |
