using System.Data;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.SqlGeneration;
using Dapper;

namespace Chef.DbAccess.SqlServer.SqlGeneration
{
    /// <summary>
    ///     把 <see cref="GeneratedSql.Parameters" /> 轉成 Dapper 的 <see cref="DynamicParameters" />，依欄位的 <c>[Column(TypeName = ...)]</c> 指定參數型別：
    ///     <list type="bullet">
    ///         <item>
    ///             字串：用 <see cref="DbString" /> 指定 ANSI 與長度，避免 <c>varchar</c> 欄位與 <c>nvarchar</c> 參數比較時發生隱含轉換而用不到索引。
    ///         </item>
    ///         <item>
    ///             <c>datetime2</c>、<c>date</c>、<c>time</c>：指定對應的 <see cref="DbType" />。不指定時 Dapper 會把 <see cref="System.DateTime" /> 一律送成
    ///             <c>datetime</c>，精確度只到約 3.33 毫秒。
    ///         </item>
    ///         <item>其他情況不指定型別，由 Dapper 依 CLR 型別決定。</item>
    ///     </list>
    ///     與 3.x 的差異：
    ///     <list type="bullet">
    ///         <item>3.x 會把非字串的值強制轉型成字串（例如 <c>[Column(TypeName = "datetime2")]</c>），導致例外。</item>
    ///         <item>型別名稱先去掉長度再判斷，<c>varchar(20)</c> 視為 <c>varchar</c>。3.x 會把它當成 <c>nvarchar</c>。</item>
    ///         <item>
    ///             值比 <c>[StringLength]</c> 長時不使用欄位長度（長度為 -1，由 Dapper 決定）。3.x 一律使用欄位長度，SqlClient 會把值截斷，
    ///             例如 <c>varchar(6)</c> 欄位用 <c>"Johnny Walker"</c> 查詢會變成比對 <c>"Johnny"</c>；LIKE 跳脫後的值也可能比欄位長。
    ///         </item>
    ///         <item>3.x 不指定日期時間的 <see cref="DbType" />。</item>
    ///     </list>
    /// </summary>
    internal static class SqlServerParameterMapper
    {
        public static DynamicParameters ToDynamicParameters(GeneratedSql generated)
        {
            var parameters = new DynamicParameters();

            foreach (var parameter in generated.Parameters)
            {
                var (value, dbType) = Map(parameter);

                parameters.Add(parameter.Name, value, dbType);
            }

            return parameters;
        }

        /// <summary>
        ///     回傳要交給 Dapper 的值，以及要指定的 <see cref="DbType" />（<c>null</c> 表示由 Dapper 依 CLR 型別決定）。
        /// </summary>
        public static (object Value, DbType? DbType) Map(SqlParameterExpression parameter)
        {
            var typeName = parameter.Property?.ColumnTypeName;

            if (typeName == null) return (parameter.Value, null);

            var baseTypeName = GetBaseTypeName(typeName);

            if (parameter.Value is string value)
            {
                // 參數的 Size 小於值的長度時，SqlClient 會直接截斷值，可能變成比對到別的資料；超過欄位長度時改由 Dapper 決定長度。
                var maxLength = parameter.Property.MaxLength;
                var length = maxLength != null && value.Length <= maxLength.Value ? maxLength.Value : -1;

                return (CreateDbString(value, baseTypeName, length), null);
            }

            switch (baseTypeName)
            {
                case "DATETIME2": return (parameter.Value, DbType.DateTime2);
                case "DATE": return (parameter.Value, DbType.Date);
                case "TIME": return (parameter.Value, DbType.Time);
                default: return (parameter.Value, null);
            }
        }

        private static DbString CreateDbString(string value, string baseTypeName, int length)
        {
            switch (baseTypeName)
            {
                case "VARCHAR": return new DbString { Value = value, Length = length, IsFixedLength = false, IsAnsi = true };
                case "CHAR": return new DbString { Value = value, Length = length, IsFixedLength = true, IsAnsi = true };
                case "NCHAR": return new DbString { Value = value, Length = length, IsFixedLength = true, IsAnsi = false };
                default: return new DbString { Value = value, Length = length, IsFixedLength = false, IsAnsi = false };
            }
        }

        private static string GetBaseTypeName(string typeName)
        {
            var parenthesis = typeName.IndexOf('(');

            return (parenthesis < 0 ? typeName : typeName.Substring(0, parenthesis)).Trim().ToUpperInvariant();
        }
    }
}
