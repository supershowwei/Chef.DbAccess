using System;
using System.Collections.Generic;
using System.Data.Common;
using Chef.DbAccess.Metadata;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     放在快取中的查詢：SQL、參數的取值方式與編譯好的 Shaper。不保留任何一次執行的參數值。
    /// </summary>
    internal sealed class CompiledQuery
    {
        public CompiledQuery(string sql, IReadOnlyList<CompiledParameter> parameters, QueryResultKind resultKind, Delegate shaper)
        {
            this.Sql = sql;
            this.Parameters = parameters;
            this.ResultKind = resultKind;
            this.Shaper = shaper;
        }

        public string Sql { get; }

        /// <summary>
        ///     依 SQL 中出現的順序。
        /// </summary>
        public IReadOnlyList<CompiledParameter> Parameters { get; }

        public QueryResultKind ResultKind { get; }

        /// <summary>
        ///     <c>Func&lt;DbDataReader, IReadOnlyDictionary&lt;string, object&gt;, T&gt;</c>；<see cref="QueryResultKind.Any" /> 與 <see cref="QueryResultKind.All" /> 為 <c>null</c>。
        /// </summary>
        public Delegate Shaper { get; }

        public Func<DbDataReader, IReadOnlyDictionary<string, object>, T> GetShaper<T>()
        {
            return (Func<DbDataReader, IReadOnlyDictionary<string, object>, T>)this.Shaper;
        }
    }

    /// <summary>
    ///     SQL 參數的名稱、型別與對應的欄位，以及依查詢參數值算出參數值的方式。
    /// </summary>
    internal sealed class CompiledParameter
    {
        public CompiledParameter(string name, Type type, PropertyMetadata property, Func<IReadOnlyDictionary<string, object>, object> binding)
        {
            this.Name = name;
            this.Type = type;
            this.Property = property;
            this.Binding = binding;
        }

        public string Name { get; }

        public Type Type { get; }

        public PropertyMetadata Property { get; }

        public Func<IReadOnlyDictionary<string, object>, object> Binding { get; }
    }
}
