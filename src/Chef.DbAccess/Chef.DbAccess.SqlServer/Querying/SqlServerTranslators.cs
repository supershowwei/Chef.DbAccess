using System.Collections.Generic;
using Chef.DbAccess.Querying;

namespace Chef.DbAccess.SqlServer.Querying
{
    /// <summary>
    ///     SQL Server 方言提供給翻譯器（<c>SqlExpressionTranslator</c>）的成員與方法翻譯器，依序嘗試。
    /// </summary>
    internal static class SqlServerTranslators
    {
        public static readonly IReadOnlyList<IMemberTranslator> Members = new IMemberTranslator[]
                                                                          {
                                                                              SqlServerStringTranslator.Instance,
                                                                              SqlServerDateTimeTranslator.Instance
                                                                          };

        public static readonly IReadOnlyList<IMethodCallTranslator> MethodCalls = new IMethodCallTranslator[]
                                                                                  {
                                                                                      SqlServerStringTranslator.Instance,
                                                                                      SqlServerDateTimeTranslator.Instance,
                                                                                      SqlServerMathTranslator.Instance
                                                                                  };
    }
}
