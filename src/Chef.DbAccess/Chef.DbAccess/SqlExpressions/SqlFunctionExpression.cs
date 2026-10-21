using System;
using System.Collections.Generic;
using System.Linq;

namespace Chef.DbAccess.SqlExpressions
{
    /// <summary>
    ///     SQL 函式呼叫，例如 <c>LEN([m].[Name])</c>、<c>COUNT(*)</c>。函式名稱由翻譯器依方言決定。
    /// </summary>
    public sealed class SqlFunctionExpression : SqlExpression
    {
        public SqlFunctionExpression(string name, IEnumerable<SqlExpression> arguments, Type type)
            : base(type)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Function name can not be null or empty.", nameof(name));

            this.Name = name;
            this.Arguments = arguments?.ToArray() ?? throw new ArgumentNullException(nameof(arguments));

            if (this.Arguments.Any(a => a == null)) throw new ArgumentException("Arguments can not contain null.", nameof(arguments));
        }

        public string Name { get; }

        public IReadOnlyList<SqlExpression> Arguments { get; }
    }
}
