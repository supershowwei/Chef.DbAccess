using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Chef.DbAccess.Querying
{
    /// <summary>
    ///     <see cref="ChefQueryable{T}" /> 的 <see cref="IQueryProvider" />：組合查詢、以 <see cref="QueryCompiler" /> 準備 SQL，再交給 <see cref="IQueryExecutor" /> 執行。
    ///     <para>
    ///         <c>First</c>、<c>Single</c> 等結果的語意在這裡以 LINQ to Objects 處理（SQL 已經加上 <c>TOP(1)</c> / <c>TOP(2)</c>），
    ///         所以沒有資料或超過一筆時的例外與 C# 相同。
    ///     </para>
    ///     <para>
    ///         <see cref="QueryCompiler" /> 由外部傳入，同一個方言的所有 Provider 共用同一份快取；
    ///         SQL 不依賴連線，所以連到不同資料庫的查詢也可以共用。
    ///     </para>
    /// </summary>
    internal sealed class ChefQueryProvider : IQueryProvider
    {
        private static readonly MethodInfo ExecuteSequenceMethod = typeof(ChefQueryProvider).GetMethods().Single(m => m.Name == nameof(ExecuteSequence) && m.IsGenericMethodDefinition);

        private static readonly MethodInfo ExecuteMethod = typeof(ChefQueryProvider).GetMethods().Single(m => m.Name == nameof(Execute) && m.IsGenericMethodDefinition);

        private readonly IQueryExecutor executor;

        public ChefQueryProvider(QueryCompiler compiler, IQueryExecutor executor)
        {
            this.Compiler = compiler ?? throw new ArgumentNullException(nameof(compiler));
            this.executor = executor ?? throw new ArgumentNullException(nameof(executor));
        }

        public QueryCompiler Compiler { get; }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new ChefQueryable<TElement>(this, expression);
        }

        public IQueryable CreateQuery(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            var elementType = GetElementType(expression.Type);

            return (IQueryable)Activator.CreateInstance(typeof(ChefQueryable<>).MakeGenericType(elementType), this, expression);
        }

        /// <summary>
        ///     一般查詢一邊讀一邊回傳；有一對多 <c>Include</c> 時要讀完所有資料列才能合併（<see cref="IdentityResolver" />）。
        /// </summary>
        public IEnumerable<T> ExecuteSequence<T>(Expression expression)
        {
            var prepared = this.Compiler.Prepare(expression);
            var rows = this.executor.Query<T>(prepared);

            return prepared.Compiled.Resolver == null ? rows : prepared.Compiled.Resolver.Resolve(rows);
        }

        /// <summary>
        ///     <c>First</c>、<c>Count</c>、<c>Any</c> 等運算子（<see cref="Queryable" /> 會呼叫這個方法）。
        /// </summary>
        public TResult Execute<TResult>(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            if (typeof(IQueryable).IsAssignableFrom(expression.Type)) return (TResult)this.ExecuteUntypedSequence(expression);

            var prepared = this.Compiler.Prepare(expression);

            switch (prepared.Compiled.ResultKind)
            {
                case QueryResultKind.First: return this.executor.Query<TResult>(prepared).First();
                case QueryResultKind.FirstOrDefault: return this.executor.Query<TResult>(prepared).FirstOrDefault();
                case QueryResultKind.Single: return this.executor.Query<TResult>(prepared).Single();
                case QueryResultKind.SingleOrDefault: return this.executor.Query<TResult>(prepared).SingleOrDefault();
                case QueryResultKind.Scalar: return this.executor.Query<TResult>(prepared).Single();
                case QueryResultKind.Any: return (TResult)(object)this.executor.HasRows(prepared);
                case QueryResultKind.All: return (TResult)(object)!this.executor.HasRows(prepared);
                default: throw new NotSupportedException($"Can not execute '{expression}' as a single result.");
            }
        }

        public async Task<List<T>> ToListAsync<T>(Expression expression, CancellationToken cancellationToken)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            var prepared = this.Compiler.Prepare(expression);
            var rows = await this.executor.QueryAsync<T>(prepared, cancellationToken).ConfigureAwait(false);

            return prepared.Compiled.Resolver == null ? rows : prepared.Compiled.Resolver.Resolve(rows);
        }

        /// <summary>
        ///     <see cref="Execute{TResult}" /> 的非同步版本，<c>FirstAsync</c>、<c>CountAsync</c> 等運算子使用。
        /// </summary>
        public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            var prepared = this.Compiler.Prepare(expression);

            switch (prepared.Compiled.ResultKind)
            {
                case QueryResultKind.First: return (await this.executor.QueryAsync<TResult>(prepared, cancellationToken).ConfigureAwait(false)).First();
                case QueryResultKind.FirstOrDefault: return (await this.executor.QueryAsync<TResult>(prepared, cancellationToken).ConfigureAwait(false)).FirstOrDefault();
                case QueryResultKind.Single: return (await this.executor.QueryAsync<TResult>(prepared, cancellationToken).ConfigureAwait(false)).Single();
                case QueryResultKind.SingleOrDefault: return (await this.executor.QueryAsync<TResult>(prepared, cancellationToken).ConfigureAwait(false)).SingleOrDefault();
                case QueryResultKind.Scalar: return (await this.executor.QueryAsync<TResult>(prepared, cancellationToken).ConfigureAwait(false)).Single();
                case QueryResultKind.Any: return (TResult)(object)await this.executor.HasRowsAsync(prepared, cancellationToken).ConfigureAwait(false);
                case QueryResultKind.All: return (TResult)(object)!await this.executor.HasRowsAsync(prepared, cancellationToken).ConfigureAwait(false);
                default: throw new NotSupportedException($"Can not execute '{expression}' as a single result.");
            }
        }

        public object Execute(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            if (typeof(IQueryable).IsAssignableFrom(expression.Type)) return this.ExecuteUntypedSequence(expression);

            return this.InvokeGeneric(ExecuteMethod.MakeGenericMethod(expression.Type), expression);
        }

        private static Type GetElementType(Type sequenceType)
        {
            var enumerable = sequenceType.IsGenericType && sequenceType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                                 ? sequenceType
                                 : sequenceType.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

            return enumerable?.GetGenericArguments()[0] ?? throw new ArgumentException($"'{sequenceType}' is not a sequence.", nameof(sequenceType));
        }

        private object ExecuteUntypedSequence(Expression expression)
        {
            return this.InvokeGeneric(ExecuteSequenceMethod.MakeGenericMethod(GetElementType(expression.Type)), expression);
        }

        private object InvokeGeneric(MethodInfo method, Expression expression)
        {
            try
            {
                return method.Invoke(this, new object[] { expression });
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }
    }
}
