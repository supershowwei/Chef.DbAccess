using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     出現在 <c>FROM</c> 裡的來源（資料表；之後會加入 Join 與子查詢）。
    /// </summary>
    public abstract class TableExpressionBase
    {
        protected TableExpressionBase(string alias)
        {
            if (string.IsNullOrEmpty(alias)) throw new ArgumentException("Alias can not be null or empty.", nameof(alias));

            this.Alias = alias;
        }

        public string Alias { get; }
    }
}
