using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Chef.DbAccess.Querying;

namespace Chef.DbAccess
{
    /// <summary>
    ///     Chef.DbAccess 查詢（<c>dataAccess.Query()</c>）的擴充方法：非同步運算子與 <see cref="ToQueryString" />。
    ///     <para>
    ///         非同步運算子組出與同步版相同的 <see cref="Queryable" /> 呼叫（例如 <c>Queryable.First(source, predicate)</c>），
    ///         所以翻譯與快取完全相同；<see cref="CancellationToken" /> 會傳給開啟連線、執行命令與讀取資料列。
    ///     </para>
    /// </summary>
    public static class ChefQueryableExtensions
    {
        private static readonly MethodInfo FirstMethod = GetMethod<object>(Queryable.First);
        private static readonly MethodInfo FirstWithPredicateMethod = GetMethodWithPredicate<object>(Queryable.First);
        private static readonly MethodInfo FirstOrDefaultMethod = GetMethod<object>(Queryable.FirstOrDefault);
        private static readonly MethodInfo FirstOrDefaultWithPredicateMethod = GetMethodWithPredicate<object>(Queryable.FirstOrDefault);
        private static readonly MethodInfo SingleMethod = GetMethod<object>(Queryable.Single);
        private static readonly MethodInfo SingleWithPredicateMethod = GetMethodWithPredicate<object>(Queryable.Single);
        private static readonly MethodInfo SingleOrDefaultMethod = GetMethod<object>(Queryable.SingleOrDefault);
        private static readonly MethodInfo SingleOrDefaultWithPredicateMethod = GetMethodWithPredicate<object>(Queryable.SingleOrDefault);
        private static readonly MethodInfo CountMethod = GetMethod<int>(Queryable.Count);
        private static readonly MethodInfo CountWithPredicateMethod = GetMethodWithPredicate<int>(Queryable.Count);
        private static readonly MethodInfo LongCountMethod = GetMethod<long>(Queryable.LongCount);
        private static readonly MethodInfo LongCountWithPredicateMethod = GetMethodWithPredicate<long>(Queryable.LongCount);
        private static readonly MethodInfo AnyMethod = GetMethod<bool>(Queryable.Any);
        private static readonly MethodInfo AnyWithPredicateMethod = GetMethodWithPredicate<bool>(Queryable.Any);
        private static readonly MethodInfo AllMethod = GetMethodWithPredicate<bool>(Queryable.All);

        /// <summary>
        ///     除錯用：不執行查詢，回傳產生的 SQL 與參數值，例如
        ///     <code>
        ///     SELECT [mem_1].[No] AS [Id], ... WHERE [mem_1].[Age] &gt; {=Age_0}
        ///
        ///     -- Parameters
        ///     -- @Age_0 = 30 (Int32)
        ///     </code>
        ///     <c>{=Age_0}</c> 是 Dapper 的字面值替換，執行時會直接寫入數字。
        /// </summary>
        /// <exception cref="InvalidOperationException"><paramref name="source" /> 不是 Chef.DbAccess 的查詢。</exception>
        /// <exception cref="NotSupportedException">查詢中有無法翻譯成 SQL 的運算式。</exception>
        public static string ToQueryString(this IQueryable source)
        {
            return GetProvider(source).Compiler.Prepare(source.Expression).ToQueryString();
        }

        public static Task<List<TSource>> ToListAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return GetProvider(source).ToListAsync<TSource>(source.Expression, cancellationToken);
        }

        public static async Task<TSource[]> ToArrayAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return (await source.ToListAsync(cancellationToken).ConfigureAwait(false)).ToArray();
        }

        /// <exception cref="InvalidOperationException">沒有資料。</exception>
        public static Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(FirstMethod, source, null, cancellationToken);
        }

        /// <exception cref="InvalidOperationException">沒有符合條件的資料。</exception>
        public static Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(FirstWithPredicateMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        public static Task<TSource> FirstOrDefaultAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(FirstOrDefaultMethod, source, null, cancellationToken);
        }

        public static Task<TSource> FirstOrDefaultAsync<TSource>(
            this IQueryable<TSource> source,
            Expression<Func<TSource, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(FirstOrDefaultWithPredicateMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        /// <exception cref="InvalidOperationException">沒有資料或超過一筆。</exception>
        public static Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(SingleMethod, source, null, cancellationToken);
        }

        /// <exception cref="InvalidOperationException">沒有或超過一筆符合條件的資料。</exception>
        public static Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(SingleWithPredicateMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        /// <exception cref="InvalidOperationException">超過一筆。</exception>
        public static Task<TSource> SingleOrDefaultAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(SingleOrDefaultMethod, source, null, cancellationToken);
        }

        /// <exception cref="InvalidOperationException">超過一筆符合條件的資料。</exception>
        public static Task<TSource> SingleOrDefaultAsync<TSource>(
            this IQueryable<TSource> source,
            Expression<Func<TSource, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, TSource>(SingleOrDefaultWithPredicateMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        public static Task<int> CountAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, int>(CountMethod, source, null, cancellationToken);
        }

        public static Task<int> CountAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, int>(CountWithPredicateMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        public static Task<long> LongCountAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, long>(LongCountMethod, source, null, cancellationToken);
        }

        public static Task<long> LongCountAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, long>(LongCountWithPredicateMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        public static Task<bool> AnyAsync<TSource>(this IQueryable<TSource> source, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, bool>(AnyMethod, source, null, cancellationToken);
        }

        public static Task<bool> AnyAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, bool>(AnyWithPredicateMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        public static Task<bool> AllAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return ExecuteAsync<TSource, bool>(AllMethod, source, predicate ?? throw new ArgumentNullException(nameof(predicate)), cancellationToken);
        }

        private static ChefQueryProvider GetProvider(IQueryable source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            if (!(source.Provider is ChefQueryProvider provider))
            {
                throw new InvalidOperationException($"This operation only supports queries created by Chef.DbAccess, but the provider is '{source.Provider.GetType()}'.");
            }

            return provider;
        }

        private static Task<TResult> ExecuteAsync<TSource, TResult>(
            MethodInfo operatorMethod,
            IQueryable<TSource> source,
            Expression predicate,
            CancellationToken cancellationToken)
        {
            var provider = GetProvider(source);
            var arguments = predicate == null ? new[] { source.Expression } : new[] { source.Expression, Expression.Quote(predicate) };

            return provider.ExecuteAsync<TResult>(Expression.Call(null, operatorMethod.MakeGenericMethod(typeof(TSource)), arguments), cancellationToken);
        }

        private static MethodInfo GetMethod<TResult>(Func<IQueryable<object>, TResult> method)
        {
            return method.GetMethodInfo().GetGenericMethodDefinition();
        }

        private static MethodInfo GetMethodWithPredicate<TResult>(Func<IQueryable<object>, Expression<Func<object, bool>>, TResult> method)
        {
            return method.GetMethodInfo().GetGenericMethodDefinition();
        }
    }
}
