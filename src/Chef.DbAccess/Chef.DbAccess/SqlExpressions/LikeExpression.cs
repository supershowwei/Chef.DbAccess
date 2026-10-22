using System;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     <c>[m].[Name] LIKE @Name_0 + '%' ESCAPE '\'</c>。
    ///     <para><see cref="EscapeChar" /> 為 <c>null</c> 時不輸出 <c>ESCAPE</c>。</para>
    /// </summary>
    public sealed class LikeExpression : SqlExpression
    {
        public LikeExpression(SqlExpression match, SqlExpression pattern, char? escapeChar = null)
            : base(typeof(bool))
        {
            this.Match = match ?? throw new ArgumentNullException(nameof(match));
            this.Pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
            this.EscapeChar = escapeChar;
        }

        public SqlExpression Match { get; }

        public SqlExpression Pattern { get; }

        public char? EscapeChar { get; }
    }
}
