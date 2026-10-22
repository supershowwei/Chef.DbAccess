using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     原封不動輸出的 SQL 片段，用在函式中的關鍵字，例如 <c>DATEPART(year, ...)</c> 的 <c>year</c>、<c>CONVERT(date, ...)</c> 的 <c>date</c>。
    ///     <para>只能放翻譯器自己決定的固定文字，絕對不能放使用者提供的值。</para>
    /// </summary>
    public sealed class SqlFragmentExpression : SqlExpression
    {
        public SqlFragmentExpression(string sql)
            : base(typeof(object))
        {
            if (string.IsNullOrEmpty(sql)) throw new ArgumentException("SQL fragment can not be null or empty.", nameof(sql));

            this.Sql = sql;
        }

        public string Sql { get; }
    }
}
