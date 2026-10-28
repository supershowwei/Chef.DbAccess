using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     把 LINQ 查詢準備成可以執行的 SQL：
    ///     <list type="number">
    ///         <item><see cref="PartialEvaluator" /> 先在用戶端算出閉包變數等值，換成參數。</item>
    ///         <item>以 <see cref="QueryCacheKey" /> 查快取；沒有命中才翻譯、產生 SQL、編譯 Shaper，放進快取。</item>
    ///         <item>依快取中的參數取值方式，用這次的參數值算出每個 SQL 參數的值。</item>
    ///     </list>
    ///     <para>
    ///         同一個執行個體可以同時給多個執行緒使用。快取依運算式形狀區分，不會因為參數值不同而增加，
    ///         但目前沒有上限，形狀非常多的應用程式要留意記憶體。
    ///     </para>
    /// </summary>
    internal sealed class QueryCompiler
    {
        private static readonly MethodInfo CompileShaperMethod = typeof(ShaperCompiler).GetMethod(nameof(ShaperCompiler.Compile));

        private readonly ISqlDialect dialect;
        private readonly ConcurrentDictionary<QueryCacheKey, CompiledQuery> cache = new ConcurrentDictionary<QueryCacheKey, CompiledQuery>();

        public QueryCompiler(ISqlDialect dialect)
        {
            this.dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
        }

        /// <summary>
        ///     目前快取的查詢數量。
        /// </summary>
        public int CacheCount => this.cache.Count;

        public PreparedQuery Prepare(Expression query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            var parameterized = PartialEvaluator.Evaluate(query);
            var compiled = this.cache.GetOrAdd(new QueryCacheKey(parameterized), _ => this.Compile(parameterized));
            var parameterValues = new QueryParameterValues(parameterized.ParameterValues);

            var parameters = compiled.Parameters.Select(p => new SqlParameterExpression(p.Name, p.Binding(parameterValues), p.Type, p.Property)).ToArray();

            return new PreparedQuery(compiled, parameters, parameterValues);
        }

        private CompiledQuery Compile(ParameterizedQuery parameterized)
        {
            var translated = new QueryableMethodTranslator(
                    new SqlTranslationContext(parameterized.ParameterValues),
                    this.dialect.MemberTranslators,
                    this.dialect.MethodCallTranslators)
                .Translate(parameterized.Expression);

            var generated = this.dialect.CreateGenerator().Generate(translated.Select);

            // 只保留參數的名稱、型別與取值方式，不保留這次執行的值。
            var parameters = generated.Parameters.Select(p => new CompiledParameter(p.Name, p.Type, p.Property, translated.ParameterBindings[p.Name])).ToArray();

            var shaper = translated.Shaper == null ? null : CompileShaper(translated.Shaper);

            return new CompiledQuery(generated.Sql, parameters, translated.ResultKind, shaper, translated.Resolver);
        }

        /// <summary>
        ///     Shaper 的型別在執行期才知道（例如匿名型別），以反射呼叫泛型的 <see cref="ShaperCompiler.Compile{T}" />。
        /// </summary>
        private static Delegate CompileShaper(Expression shaper)
        {
            try
            {
                return (Delegate)CompileShaperMethod.MakeGenericMethod(shaper.Type).Invoke(null, new object[] { shaper });
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }
    }
}
