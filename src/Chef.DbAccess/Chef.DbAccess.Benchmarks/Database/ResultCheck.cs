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
    }
}
