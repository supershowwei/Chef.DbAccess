using System;
using System.Reflection;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把屬性或欄位存取翻譯成 SQL，例如 SQL Server 把 <c>x.Name.Length</c> 翻譯成 <c>LEN([Name])</c>。由各資料庫的方言提供。
    /// </summary>
    public interface IMemberTranslator
    {
        /// <param name="instance">已翻譯的執行個體，例如 <c>[m].[Name]</c>。</param>
        /// <param name="member">被存取的成員。</param>
        /// <param name="returnType">成員的型別。</param>
        /// <returns>翻譯結果；不處理這個成員時回傳 <c>null</c>，交給下一個翻譯器。</returns>
        SqlExpression Translate(SqlExpression instance, MemberInfo member, Type returnType);
    }
}
