using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Microsoft.Extensions.Configuration;

namespace Chef.DbAccess.SqlServer.Samples
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Setup();

            new QueryBenchmark().Benchmark();

            //IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            //var memberDataAccess = dataAccessFactory.Create<Member>();

            //// 對應不同資料庫相同結構的資料表
            //var memberDataAccessOnAnotherMemberDB = dataAccessFactory.Create<Member>("AnotherMemberDB");

            //// 執行 SQL 語法的同時將 SQL 語法輸出到指定的方法之中
            //memberDataAccess.OutputSql = (sql, parameters) => Console.WriteLine(sql);

            //// Do not Dirty Read.
            //memberDataAccess.IsDirtyRead = false;
        }

        private static void Setup()
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

            // Add UserDefinedTable, and add [UserDefined(TableType = "MemberType")] on Model
            SqlServerDataAccessFactory.Instance.AddUserDefinedTable(
                "MemberType",
                new Dictionary<string, Type>
                {
                    ["Id"] = typeof(int),
                    ["Name"] = typeof(string),
                    ["Phone"] = typeof(string),
                    ["Age"] = typeof(int)
                });
        }
    }
}