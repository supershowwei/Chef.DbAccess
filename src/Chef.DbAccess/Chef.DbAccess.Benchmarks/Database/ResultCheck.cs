using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Chef.DbAccess.Benchmarks.Database
{
    /// <summary>
    /// 在 GlobalSetup 比對各寫法的結果：結果必須相同，量到的才是同一件事。
    /// </summary>
    internal static class ResultCheck
    {
        public static void AssertSame(string name, Member expected, Member actual)
        {
            AssertSame(name, new[] { expected }, new[] { actual });
        }

        public static void AssertSame(string name, IEnumerable<Member> expected, IEnumerable<Member> actual)
        {
            // 沒有 ORDER BY 的查詢不保證順序，依主鍵排序後再比對
            var expectedRows = expected.OrderBy(x => x?.Id).Select(Describe).ToList();
            var actualRows = actual.OrderBy(x => x?.Id).Select(Describe).ToList();

            AssertSame(name, expectedRows, actualRows);
        }

        /// <summary>
        /// 比對寫入的資料。Id 由資料庫產生，而 INSERT ... SELECT 不保證產生 Id 的順序，所以不比 Id，排序後逐筆比對。
        /// </summary>
        public static void AssertSame(string name, IEnumerable<MemberWrite> expected, IEnumerable<MemberWrite> actual)
        {
            var expectedRows = expected.Select(Describe).OrderBy(x => x, StringComparer.Ordinal).ToList();
            var actualRows = actual.Select(Describe).OrderBy(x => x, StringComparer.Ordinal).ToList();

            AssertSame(name, expectedRows, actualRows);
        }

        private static void AssertSame(string name, List<string> expectedRows, List<string> actualRows)
        {
            if (expectedRows.Count == 0 || expectedRows.Contains(null)) throw new InvalidOperationException($"{name}：基準寫法沒有查到資料");

            if (expectedRows.Count != actualRows.Count) throw new InvalidOperationException($"{name}：筆數不同（{expectedRows.Count}／{actualRows.Count}）");

            for (var i = 0; i < expectedRows.Count; i++)
            {
                if (expectedRows[i] != actualRows[i])
                {
                    throw new InvalidOperationException($"{name}：第 {i} 筆不同\n預期：{expectedRows[i]}\n實際：{actualRows[i]}");
                }
            }
        }

        private static string Describe(Member member)
        {
            if (member == null) return null;

            return string.Join(
                " | ",
                member.Id,
                member.Name,
                member.Email,
                member.Age,
                member.Balance.ToString(CultureInfo.InvariantCulture),
                member.IsActive,
                member.CreatedAt.ToString("O"),
                member.UpdatedAt?.ToString("O"),
                member.Token,
                member.DepartmentId,
                member.Department?.Id,
                member.Department?.Name,
                member.Department?.Company?.Id,
                member.Department?.Company?.Name);
        }

        private static string Describe(MemberWrite member)
        {
            return string.Join(
                " | ",
                member.Name,
                member.Email,
                member.Age,
                // 記憶體中的值與 DECIMAL(18, 2) 讀回的值小數位數可能不同（1.5 與 1.50），固定成兩位
                member.Balance.ToString("F2", CultureInfo.InvariantCulture),
                member.IsActive,
                member.CreatedAt.ToString("O"),
                member.UpdatedAt?.ToString("O"),
                member.Token,
                member.DepartmentId);
        }
    }
}
