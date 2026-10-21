[創意料理] 單純對資料表簡單的 CRUD 讓 Chef.DbAccess.SqlServer 來協助我們

Chef.DbAccess

Chef.DbAccess,Chef.DbAccess.SqlServer,Dapper,CRUD

[Chef.DbAccess.SqlServer](https://www.nuget.org/packages/Chef.DbAccess.SqlServer/) 這個套件是小弟本人開發的，在 ORM 工具盛行的現在，後端工程師能有一套像是 [Entity Framework](https://docs.microsoft.com/zh-tw/dotnet/framework/data/adonet/ef/overview) 的資料庫存取框架來協助開發工作，是件很幸福的事情，但是無奈因為環境的關係、政策的關係、~~仇恨的關係~~、...blah blah 的，無法盡如人意，在這種情況之下只好自立自強，於是這個套件就誕生了。

`Chef.DbAccess.SqlServer` 只有簡單陽春的 CRUD，使用上也有一些限制，是無法跟 Entity Framework 相提並論的，等級差太多，如果能使用 Entity Framework 的朋友就繼續使用 Entity Framework 就好了，文章看到這邊可以左轉出去了，但是如果還是跟我一樣需要操著 SQL 跟資料庫奮戰的朋友，說不定這個套件會對各位有點兒幫助。

適合的使用情境是這樣的，很多時候我們需要開發一些對`單一資料表`做簡單的 CRUD 存取，在分層的機制下，會多出不少簡短又相似的程式碼，我們會想說能不能將這些程式碼進一步做抽象化，讓它們更彈性一點，而這個套件的開發初衷就是希望能解決這方面的問題。

先說明一下我這次使用的範例，我有兩個資料表叫 `tbl_member`、`tbl_department`，其結構如下圖：

![](https://i.imgur.com/eCqhQE3.png)

![](https://i.imgur.com/zg2vclf.png)

與之對應的類別如下：

    [Table("tbl_member")]
    public class Member
    {
        [Column("No")]
        [Required]
        public int Id { get; set; }

        [StringLength(50)]
        [Required]
        public string Name { get; set; }

        [Column(TypeName = "varchar")]
        [StringLength(20)]
        public string Phone { get; set; }

        public int Age { get; set; }
        
        [Required]
        public int DepartmentId { get; set; }

        public Department Department { get; set; }

        [Required]
        public int ManagerId { get; set; }

        public Member Manager { get; set; }

        public List<Department> ManagedDepartments { get; set; }
        
        [NotMapped]
        public string Secret { get; set; }
    }

    [Table("tbl_member")]
    public class MemberStatistics
    {
        [Column("No")]
        public int Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }

        public decimal AverageAge { get; set; }
    }

    [Table("tbl_department")]
    public class Department
    {
        [Required]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        public int ManagerId { get; set; }
    }

如果資料表名稱跟類別名稱不一樣，可以用 `TableAttribute` 來標記，而欄位名稱跟屬性名稱不一樣，則是用 `ColumnAttribute` 來標記，另外，這個套件相依於 [Dapper](https://github.com/StackExchange/Dapper)，所以有針對[參數型別](https://dotblogs.com.tw/supershowwei/2019/08/12/232213)的語法做調整，如果欄位的資料型態是 `varchar` 的話，建議特別標記 Column 的 `TypeName`，而 `StringLengthAttribute` 則是選擇性的標記。

## 適用版本

- Chef.DbAccess.SqlServer v3.0.3+

## 起手式

首先，類別建好後要為它指定連線字串，標記 `[ConnectionString]` 告訴套件說資料表是在哪一個資料庫內，這樣基本上就可以開始下 Query 了，既然是自己寫的，當然要弄得越方便越好。

    [ConnectionString("nameOrConnectionString")]
    [Table("tbl_member")]
    public class Member
    {
        ...
    }

    [ConnectionString("nameOrConnectionString")]
    [Table("tbl_member")]
    public class MemberStatistics
    {
        ...
    }

    [ConnectionString("nameOrConnectionString")]
    [Table("tbl_department")]
    public class Department
    {
        ...
    }

連線字串可以直接是 SQL Server 的連線字串，也可以是連線字串的名稱，而透過連線字串名稱取得連線字串的方式，需要先操作 `SqlServerDataAccessFactory.Instance.AddConnectionString()` 方法，從設定檔中將連線字串加進來，這樣才會對應得到。

    internal class Program
    {
        private static void Main(string[] args)
        {
            var configurationRoot = new ConfigurationBuilder().SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location))
                .AddJsonFile("appsettings.json")
                .AddJsonFile("appsettings.Debug.json", true)
                .AddJsonFile("appsettings.Release.json", true)
                .Build();

            // 將設定檔中的連線字串加到 SqlServerDataAccessFactory
            foreach (var configurationSection in configurationRoot.GetSection("ConnectionStrings").GetChildren())
            {
                SqlServerDataAccessFactory.Instance.AddConnectionString(configurationSection.Key, configurationSection.Value);
            }
        }
    }

弄好之後，直接呼叫 `IDataAccessFactory.Create<T>()` 就可以建立 Data Access Object。

    internal class Program
    {
        private static void Main(string[] args)
        {
            ...

            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();
        }
    }

[ConnectionString] 可以標記多個，也就是說一個類別可以對應不同資料庫相同結構的資料表，只是在建立 Data Access Object 時需要指定連線字串名稱。

另外，一個類別不只能對應一個資料表，如果資料庫中有多個資料表有相同的欄位，類別能夠共用，只要在 `Create` 的時候指定資料表名稱即可。 

    [ConnectionString("MemberDB")]
    [ConnectionString("AnotherMemberDB")]
    [Table("tbl_member")]
    public class Member
    {
        ...
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            ...

            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            // 對應不同資料庫相同結構的資料表
            var memberDataAccessOnAnotherMemberDB = dataAccessFactory.Create<Member>(null, "AnotherMemberDB");

            // 對應相同結構的不同資料表
            var userDataAccessOnAnotherMemberDB = dataAccessFactory.Create<Member>("User", "AnotherMemberDB");
        }
    }

建好 Data Access Object 之後就可以開始 Query 了，Query 的方法會在下面介紹，而套件中只有非同步方法，不支援同步方法，範例程式也都是使用非同步方法。

## 輸出產生的 SQL 語法

IDataAccess 有一個型別為 Action&lt;string, IDictionary&lt;string, object&gt;&gt; 的屬性 `OutputSql`，可以在執行 SQL 語法的同時將 SQL 語法輸出到指定的方法之中。

    IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

    var memberDataAccess = dataAccessFactory.Create<Member>();
    
    memberDataAccess.OutputSql = (sql, parameters) => Console.WriteLine(sql);

## 預設為 Dirty Read

SqlServerDataAccess 預設是 `Dirty Read`，我們可以將 `IsDirtyRead` 屬性設為 false 來改變這個行為。

    IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

    var memberDataAccess = dataAccessFactory.Create<Member>();
    
    memberDataAccess.IsDirtyRead = false;



以上，針對 Chef.DbAccess.SqlServer 這個套件的使用方式做基本的介紹，分享給大家，希望能有一丁點兒的幫助。