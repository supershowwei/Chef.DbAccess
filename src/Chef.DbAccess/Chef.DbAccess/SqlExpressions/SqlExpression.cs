using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     SQL AST 中會產生值的節點（欄位、參數、常數、運算、函式……）。
    ///     <para>AST 與資料庫無關，由各資料庫的方言負責輸出成 SQL 字串。所有節點都是不可變的。</para>
    /// </summary>
    public abstract class SqlExpression
    {
        protected SqlExpression(Type type)
        {
            this.Type = type ?? throw new ArgumentNullException(nameof(type));
        }

        /// <summary>
        ///     這個節點的值對應到的 CLR 型別。
        /// </summary>
        public Type Type { get; }
    }
}
