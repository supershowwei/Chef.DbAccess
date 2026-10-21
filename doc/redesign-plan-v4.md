# Chef.DbAccess 4.0 改版規劃：邁向 IQueryable

> 狀態：草案（2026-10-21）
> 範圍：Chef.DbAccess（核心）、Chef.DbAccess.SqlServer（SQL Server 實作）

## 1. 目標與定位

把 Chef.DbAccess 從「用 Lambda 拼 SQL 的 CRUD 工具」升級成**輕量的 LINQ Provider**：

1. **不需要 DbContext**：維持現在的設計，拿一個類別就能存取資料表，`factory.Create<Member>()` 就能用。
2. **真正的 `IQueryable<T>`**：使用標準 LINQ 運算子（方法語法與查詢語法都可以）自由組合，由 Expression Tree 動態產生 SQL。
3. **保持輕量**：沒有 Change Tracking、沒有 Migration、沒有 Lazy Loading。寫入一律明確地呼叫（Insert／Update／Upsert／Delete／Bulk）。

### 目標體驗

```csharp
var members = factory.Create<Member>();
var departments = factory.Create<Department>();

// 基本查詢：條件、排序、投影到匿名型別或 DTO、分頁
var page = await members.Query()
    .Where(x => x.Age >= 30 && x.Name.StartsWith("J") && x.CreatedAt > DateTime.Today.AddDays(-7))
    .OrderBy(x => x.Age).ThenByDescending(x => x.Id)
    .Select(x => new { x.Id, x.Name, NameLength = x.Name.Length })
    .Skip(20).Take(10)
    .ToListAsync();

// Join（查詢語法），不需要事先設定關聯
var q = from m in members.Query()
        join d in departments.Query() on m.DepartmentId equals d.Id
        where d.Name.Contains("業務")
        select new { m.Name, DepartmentName = d.Name };

// GroupBy + Having
var stats = await members.Query()
    .GroupBy(x => x.DepartmentId)
    .Where(g => g.Count() > 5)
    .Select(g => new { DepartmentId = g.Key, AvgAge = g.Average(x => x.Age) })
    .ToListAsync();

// 子查詢：EXISTS / IN
var managers = await members.Query()
    .Where(m => departments.Query().Any(d => d.ManagerId == m.Id))
    .ToListAsync();

// 批次更新／刪除（set-based，不需要先把資料讀出來）
await members.Query()
    .Where(x => x.Age < 18)
    .ExecuteUpdateAsync(s => s.Set(x => x.Age, x => x.Age + 1));

// 除錯：直接看產生出來的 SQL
string sql = members.Query().Where(x => x.Id == 1).ToQueryString();
```

## 2. 現況盤點

### 2.1 值得保留的優點

- 用 `[Table]`、`[Column]`、`[Required]`、`[StringLength]`、`[NotMapped]` 等 DataAnnotations 對應，不需要額外設定。
- `[ConnectionString]` 加上 `AddConnectionString` 的連線解析機制。
- Join 結果可以填回導覽屬性（`x => x.Department`、`x => x.ManagedDepartments`），也支援一對多。
- SQL Server 專屬能力：`OUTPUT`、`MERGE` Upsert、TVP Bulk Insert／Update／Upsert、`varchar` 對應 `DbString`（避免隱含轉型）、全文檢索 `Includes()`→`CONTAINS`。
- `OutputSql`、`OnDbError` 這兩個掛鉤。

### 2.2 架構上的瓶頸

| # | 問題 | 位置 | 影響 |
|---|------|------|------|
| A1 | **泛型參數數量（arity）爆炸**：每個操作都為 1～7 張表各寫一份 | `IDataAccess.cs`（472 行）、`QueryObject.cs`、`Fluent/Extension.cs`（1,753 行）、`SqlServerDataAccess.cs`（4,697 行）、`StatementExtension.cs`（1,689 行） | 最多只能 Join 7 張表，改一個功能要改 7 份 |
| A2 | `QueryObject` 只是可變的屬性袋，**不能組合** | `QueryObject.cs` | 沒有子查詢、Union、巢狀投影；同一個物件會被就地修改 |
| A3 | 直接在字串上組 SQL，沒有中介的 SQL 模型 | `StatementExtension.ParseCondition` | 很難支援函式、運算式、子查詢，也很難換成其他資料庫 |
| A4 | 用 Regex 從**產生出來的 SQL 字串**反推欄位清單 | `SqlServerDataAccess.ResolveColumnList`、`ColumnValueRegex` | 很脆弱，遇到函式或子查詢就會壞掉 |
| A5 | 每次執行都重新解析 Expression，也沒有快取 | 全部 `Generate*Statement` | 效能浪費，反射成本每次都要付 |
| A6 | `Select` 只能投影回 `T`（`Expression<Func<T, object>>`），轉成其他型別只能在用戶端做 | `QueryAsync(cast)` | 不能直接投影成 DTO 或匿名型別 |
| A7 | 核心和 SQL Server 綁死，`Chef.DbAccess` 裡沒有方言抽象 | — | 沒辦法支援 PostgreSQL／SQLite（SQLite 拿來做測試很方便） |

### 2.3 運算式轉譯的限制（使用者會直接碰到）

- 左邊一定要是欄位：`5 < x.Age`、`x.Age + 1 > 5` 都會丟例外（`StatementExtension.cs:1164`、`:1170`）。
- 右邊只能是常數、欄位或屬性：`x.CreatedAt > DateTime.Now.AddDays(-1)`、`x.Id == GetId()` 都會丟例外（`StatementExtension.cs:1488`）。
- 不支援 `x.Name.Length`、`ToUpper()`、`Trim()`、`string.IsNullOrEmpty`、`x.CreatedAt.Year`、`Math.*`、`?:`、`??`、`HasValue`。
- `ids.Contains(x.Id)` 會展開成 `OR` 串，資料多的時候很容易超過 SQL Server 2,100 個參數的上限。
- `new int[0].Contains(x.Id)` 這種「指定長度」的陣列字面值，會把長度當成元素，產生 `[No] = 0`（`StatementExtension.ExtractArray`）。用變數傳入空集合時則正確產生 `1 = 0`。
- 3.x 的 Update、Delete、Upsert、Bulk、Insert(entity) 在呼叫 `OutputSql` 時不帶參數，除錯時看不到參數值。

### 2.4 API 陷阱（3.x 既有行為）

- `QueryObject.Where()` 會**覆蓋**先前的條件，連續呼叫兩次 `.Where()` 時，第一個條件會被悄悄丟掉（`Fluent/Extension.cs:111`）。
- 沒有先呼叫 `Where` 就呼叫 `And`／`Or`，或沒有先 `OrderBy` 就 `ThenBy`，會發生 NullReferenceException（`Fluent/Extension.cs:118`、`:168`）。
- 沒指定 `Select` 時直接丟例外，不會預設選出所有欄位（`SqlServerDataAccess.cs:2027`）。
- 全部 API 都沒有 `CancellationToken`。

### 2.5 工程面

- `Chef.DbAccess.SqlServer` 是透過 **NuGet 3.0.0** 參考 `Chef.DbAccess`，而不是 ProjectReference，改核心時沒辦法同步開發。
- 測試專案還是 .NET Framework 4.6.2 的舊式 csproj 加 `packages.config`，而且需要 LocalDB 和真實資料庫。測試大約有 280 個，但幾乎都是整合測試。
- 沒有 CI。

## 3. 目標架構

```
┌──────────────────────────── Chef.DbAccess（核心，與資料庫無關）─────────────────────────────┐
│                                                                                              │
│  IDataAccess<T> / IDataAccessFactory          ← 進入點（不需要 DbContext）                   │
│        │ .Query()                                                                            │
│        ▼                                                                                     │
│  ChefQueryable<T> : IOrderedQueryable<T>  ──►  ChefQueryProvider : IAsyncQueryProvider       │
│                                                     │                                        │
│   ① Preprocess   PartialEvaluator：可在用戶端算出的子樹先求值，閉包變數轉成參數              │
│   ② Cache        以「運算式形狀」計算結構雜湊 → 命中就直接沿用已編譯的 SQL 與 Shaper         │
│   ③ Translate    LINQ Expression ─► SQL AST（SelectExpression / Join / SqlBinary / ...）     │
│   ④ Generate     SQL AST ─► SQL 字串（透過 ISqlDialect）                                     │
│   ⑤ Shape        以 Expression 編譯 DbDataReader → T / DTO / 匿名型別 / 導覽屬性             │
│                                                                                              │
│  EntityMetadata：DataAnnotations ＋ 選用的 Fluent 設定（不需要 DbContext）                    │
│  IMemberTranslator / IMethodCallTranslator：string、DateTime、Math 等可擴充的翻譯器          │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
                 ▲ 實作
┌────── Chef.DbAccess.SqlServer ──────┐   ┌── Chef.DbAccess.Sqlite（新增；測試用／驗證抽象）──┐
│ SqlServerDialect：[ ] 跳脫、TOP、     │   │ SqliteDialect                                     │
│ OFFSET/FETCH、NOLOCK、OUTPUT、MERGE  │   └───────────────────────────────────────────────────┘
│ SqlServer 函式翻譯（LEN、DATEPART…）  │
│ TVP Bulk、DbString、全文檢索 CONTAINS │
└──────────────────────────────────────┘
```

### 3.1 關鍵設計

**進入點**
`IDataAccess<T>` 新增 `IQueryable<T> Query()`。3.x 既有的方法與 `Chef.DbAccess.Fluent` 全部保留，避免與 `System.Linq.Queryable.Where` 等方法產生多載衝突（見 §6 決策 D2）。

**SQL AST（中介模型）**
以 `SelectExpression` 為核心，包含 Projection、Tables（Table／Join／子查詢）、Predicate、GroupBy、Having、Orderings、Offset／Limit、IsDistinct。所有翻譯都先轉成 AST，最後才由方言輸出字串。AST 同時解決 A3、A4、A7：欄位清單由 AST 直接取得，不再需要用 Regex 解析。

**參數化與快取**
PartialEvaluator 把 `DateTime.Today.AddDays(-7)`、`GetId()`、閉包變數等不依賴 lambda 參數的子樹先求值，轉成 `SqlParameter`。快取的鍵是「運算式形狀」，所以 `Where(x => x.Id == id)` 不論 `id` 是多少都會命中同一份已翻譯的 SQL 與 Shaper，SQL Server 端也能重用執行計畫。

**集合 Contains → IN**
少量值用 `IN (@p0, @p1, …)`。超過門檻（例如 500 個）就改用 `OPENJSON(@json)`（SQL Server 2016 以上）或 TVP，避開 2,100 個參數的上限。

**Join 與導覽屬性**
因為沒有 DbContext，關聯需要明確指定，有三種方式：
1. 標準 LINQ：`join … on … equals …`；Left Join 用 `SelectMany` 加 `DefaultIfEmpty()`。
2. `[ForeignKey]` 慣例：`x.Department.Name` 可以直接用在 Where 或 Select，會自動產生 Join。
3. 延續 3.x 的寫法：`.Include(x => x.Department, (x, d) => x.DepartmentId == d.Id)`，把結果填回導覽屬性，一對多集合以 identity resolution 合併。

Join 兩端的 `IDataAccess` 必須是同一個連線字串，否則丟出明確的例外。跨資料庫（同一台伺服器）則沿用現有的 `[Database].[Schema].[Table]` 寫法。

**Materializer（Shaper）**
翻譯器知道每個欄位的 ordinal，所以直接把 `DbDataReader → 結果` 編譯成 delegate，支援匿名型別、DTO、巢狀導覽屬性。Dapper 只保留給 raw SQL API（`QueryAsync(sql, param)`）和參數處理。

**非同步運算子**（`Chef.DbAccess` 命名空間）
`ToListAsync`、`ToArrayAsync`、`FirstAsync`、`FirstOrDefaultAsync`、`SingleAsync`、`SingleOrDefaultAsync`、`CountAsync`、`LongCountAsync`、`AnyAsync`、`AllAsync`、`SumAsync`、`MinAsync`、`MaxAsync`、`AverageAsync`、`ExecuteUpdateAsync`、`ExecuteDeleteAsync`、`ToQueryString`，全部都接受 `CancellationToken`。

**保留 SQL Server 專屬能力，改成可以組合**
- 預設加上 `WITH (NOLOCK)`（與 3.x 的 `IsDirtyRead = true` 一致）；`.WithoutNoLock()`／`.WithHint("…")`：在單一查詢上關掉 NOLOCK 或改用其他 table hint。
- `.Where(x => x.Name.Includes("Johnny"))`：全文檢索，沿用 `Chef.DbAccess.Syntax`。
- `.SelectColumns(x => new { x.Name, x.Age })`：只取部分欄位，但仍然回傳 `T`，延續 3.x 的使用習慣。
- `InsertAsync`／`UpsertAsync`／`Bulk*Async`／`OUTPUT`：沿用 3.x 的語意，內部改用新的 AST 和 metadata 實作。

### 3.2 LINQ 支援範圍

| 類別 | 4.0 支援 | 之後 |
|------|----------|------|
| 篩選與排序 | `Where`、`OrderBy(Descending)`、`ThenBy(Descending)`、`Skip`、`Take`、`Distinct` | `Reverse` |
| 投影 | 投影到實體、匿名型別、DTO、純量；`?:`→`CASE`、`??`→`COALESCE` | 巢狀集合投影 |
| 終結運算子 | `First`、`Single`（含 `OrDefault`）、`Count`、`LongCount`、`Any`、`All` | `ElementAt`、`Last` |
| 聚合 | `Sum`、`Min`、`Max`、`Average`、`GroupBy`＋Having | 視窗函式 |
| Join | `Join`、`SelectMany`＋`DefaultIfEmpty`（Left Join）、`Include`、`[ForeignKey]` 導覽 | — |
| 集合運算 | `Contains`（IN）、`Any` 或 `All` 子查詢（EXISTS） | `Union`、`Concat`、`Intersect`、`Except` |
| 成員與方法 | string：`Contains`、`StartsWith`、`EndsWith`、`Length`、`ToUpper`、`ToLower`、`Trim`、`Substring`、`IsNullOrEmpty`；DateTime：`Year`…`Second`、`Date`、`AddDays`…；`Math.Abs`、`Round`、`Floor`、`Ceiling`；`Nullable.HasValue`、`Value`；enum；位元運算 | 使用者自訂函式對應 |
| 寫入 | `ExecuteUpdateAsync`、`ExecuteDeleteAsync`（可以搭配 Join）、Insert、Upsert、Bulk 系列、`OUTPUT` | — |
| Raw SQL | `FromSql($"… {x} …")`：插值字串自動參數化，而且可以繼續接 LINQ | — |

遇到無法翻譯的運算式時，**一律丟出明確的 `NotSupportedException`，並附上該段運算式**，絕對不在用戶端偷偷做計算（EF Core 3 之後的原則）。

### 3.3 明確不做的事（Non-goals）

Change Tracking、`SaveChanges`、Migration、Lazy Loading、Identity Map、DbContext。

## 4. 實施階段

每個階段結束時都要能出 preview 套件，讓團隊實際試用。

### Phase 0：工程整頓（先做，風險最低）✅ 已完成
- `Chef.DbAccess.SqlServer` 改成 ProjectReference `Chef.DbAccess`。
- 兩個函式庫都 multi-target `netstandard2.0;net8.0`，把 `LangVersion` 統一起來。**Dapper 維持 2.0.4**，以相容無法升級的既有專案；4.0 不能依賴更新版 Dapper 的 API。
- 測試專案改成 SDK-style 的 `net8.0`，測試分成兩類：
  - **SQL 快照測試**（不需要資料庫，比對產生出來的 SQL 和參數），這會是之後開發的主力。
  - **整合測試**：用 Testcontainers 啟動 SQL Server 或 LocalDB，以 `[TestCategory("Integration")]` 區隔。
- **特徵化測試**：先把 3.x 各種 API 產生的 SQL 全部錄成快照，作為之後相容層的回歸基準（`Chef.DbAccess.SqlServer.Tests/Characterization`，52 個快照）。
- GitHub Actions CI：build、跑快照測試、`dotnet pack`。

### Phase 1：Metadata 與 SQL AST ✅ 已完成
- `EntityMetadata`／`PropertyMetadata`：集中解析 DataAnnotations，建立一次後快取。補上 `[Key]`、`[DatabaseGenerated]`。（1.1）
- SQL AST 節點（含 Join）、`SqlGenerator`（ANSI）與 `SqlServerSqlGenerator`，以及參數型別對應（`DbString`、`datetime2`／`date`／`time`）。（1.2～1.6）
- 交付標準：可以手動組 AST，並產生正確的 T-SQL，有對應的測試。
- 延後：`[ForeignKey]` 移到 Phase 3（導覽屬性需要時再做）；全域 Fluent 設定 `ChefDbAccess.Configure<Member>(...)` 移到 4.x 之後，確定需要再做；3.x 的跨資料庫 Join（`[Database].[Schema].[Table]`）在 Phase 3 補上。

### Phase 2：IQueryable MVP（單表）
- `ChefQueryable<T>`、`ChefQueryProvider`、`PartialEvaluator`、查詢快取。
- 翻譯 `Where`、`Select`、`OrderBy`、`ThenBy`、`Skip`、`Take`、`Distinct`、`First`、`Single`、`Count`、`Any`，以及 §3.2 列出的成員與方法翻譯器。
- Shaper 編譯、非同步運算子、`ToQueryString()`、`CancellationToken`。
- `IDataAccess<T>.Query()` 上線，**出 4.0.0-preview1**。

### Phase 3：Join 與導覽屬性
- `Join`、`SelectMany`＋`DefaultIfEmpty`、`[ForeignKey]` 導覽、`Include`、一對多的 identity resolution。
- 沒有表數上限（取代 3.x 的 7 表限制）。**出 preview2**。

### Phase 4：聚合與子查詢
- `GroupBy`＋聚合＋Having、`Sum`、`Min`、`Max`、`Average`、EXISTS／IN 子查詢、Contains 的大量值策略（OPENJSON 或 TVP）。
- `Union`、`Concat`、`Intersect`、`Except`。**出 preview3**。

### Phase 5：寫入
- `ExecuteUpdateAsync`、`ExecuteDeleteAsync`（可以搭配 Join 和 `OUTPUT`）。
- 把 Insert、Upsert、Bulk 系列改寫到新的 metadata 和 AST 上。
- `FromSql` 插值參數化。

### Phase 6：相容層與正式版
- 讓 `Chef.DbAccess.Fluent` 的 `QueryObject` 改寫成在內部轉成 IQueryable 運算式，舊 API 全部走新的管線，並通過 Phase 0 錄下的特徵化測試。
- 對舊 API 標上 `[Obsolete]`（只是警告），並撰寫遷移指南。
- BenchmarkDotNet：比較 3.x、Dapper 手寫 SQL、EF Core 三者。
- **發佈 4.0.0**。

### 之後（4.x 以後）
交易與 Session（以 `AsyncLocal` 共用連線）、`ILogger` 和 `DiagnosticListener`、Compiled Query、PostgreSQL 方言，以及移除 3.x API（5.0）。

## 5. 風險與對策

| 風險 | 對策 |
|------|------|
| 自己寫 LINQ Provider 的工作量大、邊界情況多 | 以 Phase 切分，每個 Phase 都有可用的 preview；參考 EF Core 的 `SelectExpression` 和 `QueryableMethodTranslatingExpressionVisitor` 的分層方式，但只做需要的範圍 |
| 原本的錯誤在編譯期就會發現，改用 IQueryable 後變成執行期才發現 | 無法翻譯時丟出附帶運算式的明確例外；提供 `ToQueryString()`；可以考慮之後加 Roslyn Analyzer |
| 改寫後 3.x 使用者行為改變 | Phase 0 的特徵化快照測試、Phase 6 的相容層、`[Obsolete]` 只發警告 |
| 效能退步 | 查詢快取、編譯好的 Shaper、在 Phase 6 跑 benchmark 確認 |

## 6. 需要你拍板的決策

| # | 決策 | 建議 |
|---|------|------|
| D1 | 走完整的 `IQueryable<T>` Provider，或者維持自訂的型別化 Builder | **完整的 IQueryable**：符合目標，可以組合，也能直接用 LINQ 查詢語法 |
| D2 | 進入點：新增 `dataAccess.Query()`，或讓 `IDataAccess<T>` 直接實作 `IQueryable<T>` | **4.x 先用 `Query()`**：完全不會跟 `Chef.DbAccess.Fluent` 的 `Where` 等擴充方法發生多載衝突；5.0 移除舊 API 時再考慮讓它直接是 IQueryable |
| D3 | 3.x API 的去留 | **4.x 保留並改走新管線，標 `[Obsolete]`，5.0 才移除** |
| D4 | 沒有 `Select` 時的預設行為 | **新 API 預設選出所有對應欄位**（和 EF 一樣）；舊 API 維持原本的行為 |
| D5 | `NOLOCK` 預設值 | ✅ **已決定：新舊 API 都預設加上**。使用者只有作者與同事，情境通常允許 Dirty Read；需要時再明確關掉 |
| D6 | 目標框架 | **`netstandard2.0` 加 `net8.0`**，保留給 .NET Framework 的使用者 |
| D7 | 是否同時做 SQLite 方言 | **建議在 Phase 2 之後加入**：用來驗證抽象設計，也可以讓部分整合測試不需要 SQL Server |
