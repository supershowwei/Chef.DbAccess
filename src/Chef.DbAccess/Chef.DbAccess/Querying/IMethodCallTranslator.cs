using System.Collections.Generic;
using System.Reflection;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把方法呼叫翻譯成 SQL，例如 SQL Server 把 <c>x.Name.ToUpper()</c> 翻譯成 <c>UPPER([Name])</c>。由各資料庫的方言提供。
    /// </summary>
    public interface IMethodCallTranslator
    {
        /// <param name="instance">已翻譯的執行個體；靜態方法時為 <c>null</c>。</param>
        /// <param name="method">被呼叫的方法，用來判斷多載。</param>
        /// <param name="arguments">已翻譯的引數，常數與閉包變數已經是參數。</param>
        /// <returns>翻譯結果；不處理這個方法時回傳 <c>null</c>，交給下一個翻譯器。</returns>
        SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments);
    }
}
