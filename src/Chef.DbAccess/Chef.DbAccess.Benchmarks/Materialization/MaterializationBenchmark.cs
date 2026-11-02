using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Dapper;

namespace Chef.DbAccess.Benchmarks.Materialization
{
    /// <summary>
    /// §10.2「物件產生」：假的 DbDataReader，1 萬筆 × 10 欄。
    /// 3.x 產生物件就是 Dapper，所以 Dapper 的數字同時是 3.x 的基準；手寫的強型別讀取是理論下限，供 5.0 的 Shaper 對照。
    /// </summary>
    [MemoryDiagnoser]
    public class MaterializationBenchmark
    {
        private const int RowCount = 10_000;

        private FakeColumn[] columns;

        [GlobalSetup]
        public void Setup()
        {
            this.columns = CreateColumns(RowCount);

            // 兩種讀法的結果必須相同，量到的才是同一件事
            var expected = this.Dapper();
            var actual = this.HandWritten();

            for (var i = 0; i < RowCount; i++)
            {
                if (!IsSameRow(expected[i], actual[i])) throw new InvalidOperationException($"第 {i} 筆的結果不同");
            }
        }

        [Benchmark(Baseline = true)]
        public List<BenchmarkRow> Dapper()
        {
            return new FakeDbDataReader(this.columns, RowCount).Parse<BenchmarkRow>().ToList();
        }

        [Benchmark]
        public List<BenchmarkRow> HandWritten()
        {
            var reader = new FakeDbDataReader(this.columns, RowCount);
            var rows = new List<BenchmarkRow>();

            while (reader.Read())
            {
                rows.Add(
                    new BenchmarkRow
                    {
                        Id = reader.GetInt32(0),
                        Name = reader.GetString(1),
                        Email = reader.IsDBNull(2) ? null : reader.GetString(2),
                        Age = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                        Balance = reader.GetDecimal(4),
                        IsActive = reader.GetBoolean(5),
                        CreatedAt = reader.GetDateTime(6),
                        UpdatedAt = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                        Token = reader.GetGuid(8),
                        Score = reader.GetInt64(9)
                    });
            }

            return rows;
        }

        private static bool IsSameRow(BenchmarkRow x, BenchmarkRow y)
        {
            return x.Id == y.Id && x.Name == y.Name && x.Email == y.Email && x.Age == y.Age && x.Balance == y.Balance
                   && x.IsActive == y.IsActive && x.CreatedAt == y.CreatedAt && x.UpdatedAt == y.UpdatedAt && x.Token == y.Token
                   && x.Score == y.Score;
        }

        private static Guid NextGuid(Random random)
        {
            var bytes = new byte[16];

            random.NextBytes(bytes);

            return new Guid(bytes);
        }

        // 欄位型別混合常見的 SQL Server 型別；可為 NULL 的欄位每 3 筆有 1 筆是 NULL
        private static FakeColumn[] CreateColumns(int rowCount)
        {
            var random = new Random(1);
            var nulls = Enumerable.Range(0, rowCount).Select(i => i % 3 == 0).ToArray();
            var start = new DateTime(2026, 1, 1);

            return new FakeColumn[]
                   {
                       new FakeColumn<int>("Id", "int", Enumerable.Range(1, rowCount).ToArray()),
                       new FakeColumn<string>("Name", "nvarchar", Enumerable.Range(0, rowCount).Select(i => $"Name {i}").ToArray()),
                       new FakeColumn<string>("Email", "varchar", Enumerable.Range(0, rowCount).Select(i => nulls[i] ? null : $"user{i}@example.com").ToArray(), nulls),
                       new FakeColumn<int>("Age", "int", Enumerable.Range(0, rowCount).Select(_ => random.Next(18, 80)).ToArray(), nulls),
                       new FakeColumn<decimal>("Balance", "decimal", Enumerable.Range(0, rowCount).Select(_ => random.Next(0, 1_000_000) / 100m).ToArray()),
                       new FakeColumn<bool>("IsActive", "bit", Enumerable.Range(0, rowCount).Select(i => i % 2 == 0).ToArray()),
                       new FakeColumn<DateTime>("CreatedAt", "datetime2", Enumerable.Range(0, rowCount).Select(i => start.AddMinutes(i)).ToArray()),
                       new FakeColumn<DateTime>("UpdatedAt", "datetime2", Enumerable.Range(0, rowCount).Select(i => start.AddMinutes(i).AddDays(1)).ToArray(), nulls),
                       new FakeColumn<Guid>("Token", "uniqueidentifier", Enumerable.Range(0, rowCount).Select(_ => NextGuid(random)).ToArray()),
                       new FakeColumn<long>("Score", "bigint", Enumerable.Range(0, rowCount).Select(i => (long)i * 1_000_003).ToArray())
                   };
        }
    }

    public class BenchmarkRow
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Email { get; set; }

        public int? Age { get; set; }

        public decimal Balance { get; set; }

        public bool IsActive { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? UpdatedAt { get; set; }

        public Guid Token { get; set; }

        public long Score { get; set; }
    }
}
