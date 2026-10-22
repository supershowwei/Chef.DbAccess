using System.Collections.Generic;
using Chef.DbAccess.SqlGeneration;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     資料庫方言提供給查詢管線的部分：SQL 產生器，以及成員與方法的翻譯器。
    /// </summary>
    internal interface ISqlDialect
    {
        IReadOnlyList<IMemberTranslator> MemberTranslators { get; }

        IReadOnlyList<IMethodCallTranslator> MethodCallTranslators { get; }

        /// <summary>
        ///     <see cref="SqlGenerator" /> 不是執行緒安全的，每次產生 SQL 都建立新的執行個體。
        /// </summary>
        SqlGenerator CreateGenerator();
    }
}
