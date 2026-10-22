using System;
using System.Text;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     LIKE 的跳脫規則：<c>%</c>、<c>_</c>、<c>[</c>（SQL Server 的字元範圍）與跳脫字元本身前面加上 <c>\</c>，並在 SQL 加上 <c>ESCAPE '\'</c>。
    ///     <para>
    ///         值裡沒有這些字元時不跳脫、也不加 <c>ESCAPE</c>，所以同一個查詢會依值產生兩種 SQL；
    ///         <see cref="QueryCacheKey" /> 因此也要記錄哪些字串參數需要跳脫。
    ///     </para>
    /// </summary>
    internal static class LikePattern
    {
        public const char EscapeChar = '\\';

        // 跳脫字元必須排在第一個：在 SQL 中用 REPLACE 跳脫時，才不會重複跳脫後面加上的跳脫字元。
        public static readonly char[] SpecialChars = { EscapeChar, '%', '_', '[' };

        public static bool NeedsEscape(string value)
        {
            return value != null && value.IndexOfAny(SpecialChars) >= 0;
        }

        public static string Escape(string value)
        {
            if (!NeedsEscape(value)) return value;

            var escaped = new StringBuilder(value.Length + 4);

            foreach (var c in value)
            {
                if (Array.IndexOf(SpecialChars, c) >= 0) escaped.Append(EscapeChar);

                escaped.Append(c);
            }

            return escaped.ToString();
        }
    }
}
