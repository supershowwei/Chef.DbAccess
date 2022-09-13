using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Chef.DbAccess.SqlServer.Extensions;
using Dapper;

[assembly: InternalsVisibleTo("Chef.DbAccess.SqlServer.Tests")]

namespace Chef.DbAccess.SqlServer
{
    public abstract class SqlServerDataAccess
    {
        protected static readonly Regex ColumnValueRegex = new Regex(@"(\[[^\]]+\]) [^\s]+ ([_0-9a-zA-Z]+\.)?([@\{\[]=?[^#;,\s\}\]\)]+(_[\d]+)?\]?\}?)", RegexOptions.Compiled);
        protected static readonly Regex ColumnRegex = new Regex(@"\[[^\]]+\]", RegexOptions.Compiled);
        protected static readonly Regex ParameterRegex = new Regex(@"@([^#;,\s\}\]\)]+)", RegexOptions.Compiled);

        protected SqlServerDataAccess()
        {
        }
    }

    public partial class SqlServerDataAccess<T> : SqlServerDataAccess, IDataAccess<T>
    {
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> RequiredColumns = new ConcurrentDictionary<Type, PropertyInfo[]>();
        private static readonly ConcurrentDictionary<string, Delegate> Setters = new ConcurrentDictionary<string, Delegate>();
        private static readonly Regex ServerRegex = new Regex(@"(Server|Data Source)=[\s]*([^;]+)[\s]*;", RegexOptions.IgnoreCase);
        private static readonly Regex DatabaseRegex = new Regex(@"(Database|Initial Catalog)=[\s]*([^;]+)[\s]*;", RegexOptions.IgnoreCase);
        private readonly string connectionString;
        private readonly string tableName;
        private readonly string alias;

        public SqlServerDataAccess(string connectionString)
        {
            this.connectionString = connectionString;

            this.tableName = typeof(T).GetCustomAttribute<TableAttribute>()?.Name ?? typeof(T).Name;
            this.alias = GenerateAlias(typeof(T), 1);

            this.IsDirtyRead = true;
        }

        public bool IsDirtyRead { get; set; }

        public Action<string, IDictionary<string, object>> OutputSql { get; set; }

        public virtual Task<int> ExecuteAsync(string sql, object param)
        {
            return this.ExecuteCommandAsync(sql, param);
        }

        public virtual Task<int> DeleteAsync(Expression<Func<T, bool>> predicate)
        {
            var (sql, parameters) = this.GenerateDeleteStatement(predicate);

            return this.ExecuteCommandAsync(sql, parameters);
        }

        protected virtual async Task<TResult> ExecuteQueryOneAsync<TResult>(string sql, object param)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = await db.QuerySingleOrDefaultAsync<TResult>(sql, param);

                return result;
            }
        }

        protected virtual async Task<TResult> ExecuteTransactionalQueryOneAsync<TResult>(string sql, object param)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";

            TResult result;
            using (var db = new SqlConnection(this.connectionString))
            {
                await db.OpenAsync();

                using (var tx = db.BeginTransaction())
                {
                    try
                    {
                        result = await db.QuerySingleOrDefaultAsync<TResult>(sql, param, transaction: tx);

                        tx.Commit();
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }

                    return result;
                }
            }
        }

        protected virtual async Task<List<TResult>> ExecuteQueryAsync<TResult>(string sql, object param)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = await db.QueryAsync<TResult>(sql, param);

                return result.ToList();
            }
        }

        protected virtual async Task<List<TResult>> ExecuteTransactionalQueryAsync<TResult>(string sql, object param)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";

            IEnumerable<TResult> result;
            using (var db = new SqlConnection(this.connectionString))
            {
                await db.OpenAsync();

                using (var tx = db.BeginTransaction())
                {
                    try
                    {
                        result = await db.QueryAsync<TResult>(sql, param, transaction: tx);

                        tx.Commit();
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }

                    return result.ToList();
                }
            }
        }

        protected virtual async Task<List<TResult>> ExecuteQueryAsync<TResult>(
            string sql,
            object param,
            string resultSql,
            object resultParam,
            string preSql = null,
            object preParam = null,
            string postSql = null,
            object postParam = null)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";
            resultSql += $"\r\n--{resultSql.MD5()}\r\n";

            using (var db = new SqlConnection(this.connectionString))
            {
                await db.OpenAsync();

                if (!string.IsNullOrEmpty(preSql))
                {
                    preSql += $"\r\n--{preSql.MD5()}\r\n";

                    await db.ExecuteAsync(preSql, preParam);
                }

                await db.ExecuteAsync(sql, param);

                var result = await db.QueryAsync<TResult>(resultSql, resultParam);

                if (!string.IsNullOrEmpty(postSql))
                {
                    postSql += $"\r\n--{postSql.MD5()}\r\n";

                    await db.ExecuteAsync(postSql, postParam);
                }

                return result.ToList();
            }
        }

        protected virtual async Task<List<TResult>> ExecuteTransactionalQueryAsync<TResult>(
            string sql,
            object param,
            string resultSql,
            object resultParam,
            string preSql = null,
            object preParam = null,
            string postSql = null,
            object postParam = null)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";
            resultSql += $"\r\n--{resultSql.MD5()}\r\n";

            IEnumerable<TResult> result;
            using (var db = new SqlConnection(this.connectionString))
            {
                await db.OpenAsync();

                using (var tx = db.BeginTransaction())
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(preSql))
                        {
                            preSql += $"\r\n--{preSql.MD5()}\r\n";

                            await db.ExecuteAsync(preSql, preParam, transaction: tx);
                        }
                        
                        await db.ExecuteAsync(sql, param, transaction: tx);

                        result = await db.QueryAsync<TResult>(resultSql, resultParam, transaction: tx);

                        tx.Commit();
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                    finally
                    {
                        if (!string.IsNullOrEmpty(postSql))
                        {
                            postSql += $"\r\n--{postSql.MD5()}\r\n";

                            await db.ExecuteAsync(postSql, postParam);
                        }
                    }

                    return result.ToList();
                }
            }
        }

        protected virtual async Task<int> ExecuteCommandAsync(string sql, object param)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";

            using (var db = new SqlConnection(this.connectionString))
            {
                var result = await db.ExecuteAsync(sql, param);

                return result;
            }
        }

        protected virtual async Task<int> ExecuteTransactionalCommandAsync(string sql, object param)
        {
            sql += $"\r\n--{sql.MD5()}\r\n";

            int result;
            using (var db = new SqlConnection(this.connectionString))
            {
                await db.OpenAsync();

                using (var tx = db.BeginTransaction())
                {
                    try
                    {
                        result = await db.ExecuteAsync(sql, param, transaction: tx);

                        tx.Commit();
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }

            return result;
        }

        private static string GenerateAlias(Type type, int suffixNo)
        {
            var alias = Regex.Replace(type.Name, "[^A-Z]", string.Empty).ToLower();

            if (string.IsNullOrEmpty(alias) || alias.Length < 2) alias = type.Name.Left(3).ToLower();

            alias = string.Concat(alias, "_", suffixNo.ToString());

            return alias;
        }

        private static Action<T, TSecond> GetOrCreateSetter<TSecond>(Expression<Func<T, TSecond>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ") -> [0]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[] { instanceParam, argumentParam };

                        var block = Expression.Block(
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType))));

                        return Expression.Lambda<Action<T, TSecond>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond> GetOrCreateSetter<TSecond>(Expression<Func<T, List<TSecond>>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ") -> [0]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentType = propertyInfo.PropertyType.GetGenericArguments()[0];
                        var argumentParam = Expression.Parameter(argumentType);

                        var parameters = new[] { instanceParam, argumentParam };

                        var propertyExpr = Expression.Property(instanceParam, propertyInfo);
                        var argumentVar = Expression.Variable(argumentType);

                        var block = Expression.Block(
                            new[] { argumentVar },
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Block(
                                    Expression.IfThen(
                                        Expression.Equal(propertyExpr, Expression.Constant(null, propertyInfo.PropertyType)),
                                        Expression.Assign(propertyExpr, Expression.New(propertyInfo.PropertyType))),
                                    Expression.IfThen(
                                        Expression.NotEqual(argumentParam, Expression.Constant(null, argumentType)),
                                        Expression.Block(
                                            Expression.Assign(argumentVar, Expression.Convert(argumentParam, argumentType)),
                                            Expression.IfThen(
                                                Expression.IsFalse(
                                                    Expression.Call(
                                                        propertyExpr,
                                                        propertyInfo.PropertyType.GetMethod("Contains", new[] { argumentType }),
                                                        argumentVar)),
                                                Expression.Call(
                                                    propertyExpr,
                                                    propertyInfo.PropertyType.GetMethod("Add"),
                                                    argumentVar)))))));

                        return Expression.Lambda<Action<T, TSecond>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird> GetOrCreateSetter<TSecond, TThird>(Expression<Func<T, TSecond, TThird>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentParam
                                         };

                        var block = Expression.Block(
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType))));

                        return Expression.Lambda<Action<T, TSecond, TThird>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird> GetOrCreateSetter<TSecond, TThird>(Expression<Func<T, TSecond, List<TThird>>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentType = propertyInfo.PropertyType.GetGenericArguments()[0];
                        var argumentParam = Expression.Parameter(argumentType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentParam
                                         };

                        var propertyExpr = Expression.Property(instanceParam, propertyInfo);
                        var argumentVar = Expression.Variable(argumentType);

                        var block = Expression.Block(
                            new[] { argumentVar },
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Block(
                                    Expression.IfThen(
                                        Expression.Equal(propertyExpr, Expression.Constant(null, propertyInfo.PropertyType)),
                                        Expression.Assign(propertyExpr, Expression.New(propertyInfo.PropertyType))),
                                    Expression.IfThen(
                                        Expression.NotEqual(argumentParam, Expression.Constant(null, argumentType)),
                                        Expression.Block(
                                            Expression.Assign(argumentVar, Expression.Convert(argumentParam, argumentType)),
                                            Expression.IfThen(
                                                Expression.IsFalse(
                                                    Expression.Call(
                                                        propertyExpr,
                                                        propertyInfo.PropertyType.GetMethod("Contains", new[] { argumentType }),
                                                        argumentVar)),
                                                Expression.Call(
                                                    propertyExpr,
                                                    propertyInfo.PropertyType.GetMethod("Add"),
                                                    argumentVar)))))));

                        return Expression.Lambda<Action<T, TSecond, TThird>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth> GetOrCreateSetter<TSecond, TThird, TFourth>(Expression<Func<T, TSecond, TThird, TFourth>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentParam
                                         };

                        var block = Expression.Block(
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth> GetOrCreateSetter<TSecond, TThird, TFourth>(Expression<Func<T, TSecond, TThird, List<TFourth>>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentType = propertyInfo.PropertyType.GetGenericArguments()[0];
                        var argumentParam = Expression.Parameter(argumentType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentParam
                                         };

                        var propertyExpr = Expression.Property(instanceParam, propertyInfo);
                        var argumentVar = Expression.Variable(argumentType);

                        var block = Expression.Block(
                            new[] { argumentVar },
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Block(
                                    Expression.IfThen(
                                        Expression.Equal(propertyExpr, Expression.Constant(null, propertyInfo.PropertyType)),
                                        Expression.Assign(propertyExpr, Expression.New(propertyInfo.PropertyType))),
                                    Expression.IfThen(
                                        Expression.NotEqual(argumentParam, Expression.Constant(null, argumentType)),
                                        Expression.Block(
                                            Expression.Assign(argumentVar, Expression.Convert(argumentParam, argumentType)),
                                            Expression.IfThen(
                                                Expression.IsFalse(
                                                    Expression.Call(
                                                        propertyExpr,
                                                        propertyInfo.PropertyType.GetMethod("Contains", new[] { argumentType }),
                                                        argumentVar)),
                                                Expression.Call(
                                                    propertyExpr,
                                                    propertyInfo.PropertyType.GetMethod("Add"),
                                                    argumentVar)))))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth>(Expression<Func<T, TSecond, TThird, TFourth, TFifth>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentParam
                                         };

                        var block = Expression.Block(
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth>(Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentType = propertyInfo.PropertyType.GetGenericArguments()[0];
                        var argumentParam = Expression.Parameter(argumentType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentParam
                                         };

                        var propertyExpr = Expression.Property(instanceParam, propertyInfo);
                        var argumentVar = Expression.Variable(argumentType);

                        var block = Expression.Block(
                            new[] { argumentVar },
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Block(
                                    Expression.IfThen(
                                        Expression.Equal(propertyExpr, Expression.Constant(null, propertyInfo.PropertyType)),
                                        Expression.Assign(propertyExpr, Expression.New(propertyInfo.PropertyType))),
                                    Expression.IfThen(
                                        Expression.NotEqual(argumentParam, Expression.Constant(null, argumentType)),
                                        Expression.Block(
                                            Expression.Assign(argumentVar, Expression.Convert(argumentParam, argumentType)),
                                            Expression.IfThen(
                                                Expression.IsFalse(
                                                    Expression.Call(
                                                        propertyExpr,
                                                        propertyInfo.PropertyType.GetMethod("Contains", new[] { argumentType }),
                                                        argumentVar)),
                                                Expression.Call(
                                                    propertyExpr,
                                                    propertyInfo.PropertyType.GetMethod("Add"),
                                                    argumentVar)))))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth, TSixth> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth, TSixth>(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ", ", typeof(TSixth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth, TSixth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentIndex == 4 ? instanceParam : Expression.Parameter(typeof(TFifth)),
                                             argumentParam
                                         };

                        var block = Expression.Block(
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth, TSixth>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth, TSixth> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth, TSixth>(Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ", ", typeof(TSixth).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth, TSixth>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentType = propertyInfo.PropertyType.GetGenericArguments()[0];
                        var argumentParam = Expression.Parameter(argumentType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentIndex == 4 ? instanceParam : Expression.Parameter(typeof(TFifth)),
                                             argumentParam
                                         };

                        var propertyExpr = Expression.Property(instanceParam, propertyInfo);
                        var argumentVar = Expression.Variable(argumentType);

                        var block = Expression.Block(
                            new[] { argumentVar },
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Block(
                                    Expression.IfThen(
                                        Expression.Equal(propertyExpr, Expression.Constant(null, propertyInfo.PropertyType)),
                                        Expression.Assign(propertyExpr, Expression.New(propertyInfo.PropertyType))),
                                    Expression.IfThen(
                                        Expression.NotEqual(argumentParam, Expression.Constant(null, argumentType)),
                                        Expression.Block(
                                            Expression.Assign(argumentVar, Expression.Convert(argumentParam, argumentType)),
                                            Expression.IfThen(
                                                Expression.IsFalse(
                                                    Expression.Call(
                                                        propertyExpr,
                                                        propertyInfo.PropertyType.GetMethod("Contains", new[] { argumentType }),
                                                        argumentVar)),
                                                Expression.Call(
                                                    propertyExpr,
                                                    propertyInfo.PropertyType.GetMethod("Add"),
                                                    argumentVar)))))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth, TSixth>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ", ", typeof(TSixth).FullName, ", ", typeof(TSeventh).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentParam = Expression.Parameter(propertyInfo.PropertyType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentIndex == 4 ? instanceParam : Expression.Parameter(typeof(TFifth)),
                                             argumentIndex == 5 ? instanceParam : Expression.Parameter(typeof(TSixth)),
                                             argumentParam
                                         };

                        var block = Expression.Block(
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Call(
                                    instanceParam,
                                    propertyInfo.GetSetMethod(),
                                    Expression.Convert(argumentParam, propertyInfo.PropertyType))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>(block, parameters).Compile();
                    });

            return setter;
        }

        private static Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh> GetOrCreateSetter<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>> lambdaExpr)
        {
            var memberExpr = (MemberExpression)lambdaExpr.Body;

            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var argumentIndex = lambdaExpr.Parameters.FindIndex(x => x.Name == parameterExpr.Name);

            var propertyInfo = (PropertyInfo)memberExpr.Member;

            var setterKey = string.Concat("(", typeof(T).FullName, ", ", typeof(TSecond).FullName, ", ", typeof(TThird).FullName, ", ", typeof(TFourth).FullName, ", ", typeof(TFifth).FullName, ", ", typeof(TSixth).FullName, ", ", typeof(TSeventh).FullName, ") -> [", argumentIndex, "]:", propertyInfo.DeclaringType.FullName, ".", propertyInfo.Name);

            var setter = (Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>)Setters.GetOrAdd(
                setterKey,
                key =>
                    {
                        var instanceParam = Expression.Parameter(propertyInfo.DeclaringType);
                        var argumentType = propertyInfo.PropertyType.GetGenericArguments()[0];
                        var argumentParam = Expression.Parameter(argumentType);

                        var parameters = new[]
                                         {
                                             argumentIndex == 0 ? instanceParam : Expression.Parameter(typeof(T)),
                                             argumentIndex == 1 ? instanceParam : Expression.Parameter(typeof(TSecond)),
                                             argumentIndex == 2 ? instanceParam : Expression.Parameter(typeof(TThird)),
                                             argumentIndex == 3 ? instanceParam : Expression.Parameter(typeof(TFourth)),
                                             argumentIndex == 4 ? instanceParam : Expression.Parameter(typeof(TFifth)),
                                             argumentIndex == 5 ? instanceParam : Expression.Parameter(typeof(TSixth)),
                                             argumentParam
                                         };

                        var propertyExpr = Expression.Property(instanceParam, propertyInfo);
                        var argumentVar = Expression.Variable(argumentType);

                        var block = Expression.Block(
                            new[] { argumentVar },
                            Expression.IfThen(
                                Expression.NotEqual(instanceParam, Expression.Constant(null, propertyInfo.DeclaringType)),
                                Expression.Block(
                                    Expression.IfThen(
                                        Expression.Equal(propertyExpr, Expression.Constant(null, propertyInfo.PropertyType)),
                                        Expression.Assign(propertyExpr, Expression.New(propertyInfo.PropertyType))),
                                    Expression.IfThen(
                                        Expression.NotEqual(argumentParam, Expression.Constant(null, argumentType)),
                                        Expression.Block(
                                            Expression.Assign(argumentVar, Expression.Convert(argumentParam, argumentType)),
                                            Expression.IfThen(
                                                Expression.IsFalse(
                                                    Expression.Call(
                                                        propertyExpr,
                                                        propertyInfo.PropertyType.GetMethod("Contains", new[] { argumentType }),
                                                        argumentVar)),
                                                Expression.Call(
                                                    propertyExpr,
                                                    propertyInfo.PropertyType.GetMethod("Add"),
                                                    argumentVar)))))));

                        return Expression.Lambda<Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>(block, parameters)
                            .Compile();
                    });

            return setter;
        }

        private static (string, string) ResolveColumnList(string sql)
        {
            var columnList = new Dictionary<string, string>();

            foreach (var match in ColumnValueRegex.Matches(sql).Cast<Match>())
            {
                if (columnList.ContainsKey(match.Groups[1].Value)) continue;

                columnList.Add(match.Groups[1].Value, match.Groups[3].Value);
            }

            return (string.Join(", ", columnList.Keys), string.Join(", ", columnList.Values));
        }

        private (string, IDictionary<string, object>) GenerateQueryStatement(
            string tableName,
            string alias,
            Expression<Func<T, bool>> predicate,
            IEnumerable<(Expression<Func<T, object>>, Sortord)> orderings = null,
            Expression<Func<T, object>> selector = null,
            Expression<Func<T, object>> groupingColumns = null,
            Expression<Func<Grouping<T>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            SqlBuilder sql = $@"
SELECT {(distinct ? "DISTINCT " : string.Empty)}{(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(new[] { alias });
            }
            else if (selector != null)
            {
                sql += selector.ToSelectList(alias);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(alias, parameters, null);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(new[] { alias });

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? string.Empty : alias);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";


            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>) GenerateQueryStatement<TSecond>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, object>> selector = null,
            Expression<Func<T, TSecond, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2) };

            SqlBuilder sql = $@"
SELECT {(distinct ? "DISTINCT " : string.Empty)}{(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = secondJoin.Item2 != null ? GetOrCreateSetter(secondJoin.Item2) : GetOrCreateSetter(secondJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>) GenerateQueryStatement<TSecond, TThird>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, object>> selector = null,
            Expression<Func<T, TSecond, TThird, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3) };

            SqlBuilder sql = $@"
SELECT {(distinct ? "DISTINCT " : string.Empty)}{(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = secondJoin.Item2 != null ? GetOrCreateSetter(secondJoin.Item2) : GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = thirdJoin.Item2 != null ? GetOrCreateSetter(thirdJoin.Item2) : GetOrCreateSetter(thirdJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>) GenerateQueryStatement<TSecond, TThird, TFourth>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4) };

            SqlBuilder sql = $@"
SELECT {(distinct ? "DISTINCT " : string.Empty)}{(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = secondJoin.Item2 != null ? GetOrCreateSetter(secondJoin.Item2) : GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = thirdJoin.Item2 != null ? GetOrCreateSetter(thirdJoin.Item2) : GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = fourthJoin.Item2 != null ? GetOrCreateSetter(fourthJoin.Item2) : GetOrCreateSetter(fourthJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>, Action<T, TSecond, TThird, TFourth, TFifth>) GenerateQueryStatement<TSecond, TThird, TFourth, TFifth>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4), GenerateAlias(typeof(TFifth), 5) };

            SqlBuilder sql = $@"
SELECT {(distinct ? "DISTINCT " : string.Empty)}{(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = secondJoin.Item2 != null ? GetOrCreateSetter(secondJoin.Item2) : GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = thirdJoin.Item2 != null ? GetOrCreateSetter(thirdJoin.Item2) : GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = fourthJoin.Item2 != null ? GetOrCreateSetter(fourthJoin.Item2) : GetOrCreateSetter(fourthJoin.Item1);
            var fifthSetter = fifthJoin.Item2 != null ? GetOrCreateSetter(fifthJoin.Item2) : GetOrCreateSetter(fifthJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>, Action<T, TSecond, TThird, TFourth, TFifth>, Action<T, TSecond, TThird, TFourth, TFifth, TSixth>) GenerateQueryStatement<TSecond, TThird, TFourth, TFifth, TSixth>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4), GenerateAlias(typeof(TFifth), 5), GenerateAlias(typeof(TSixth), 6) };

            SqlBuilder sql = $@"
SELECT {(distinct ? "DISTINCT " : string.Empty)}{(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item3, sixthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = secondJoin.Item2 != null ? GetOrCreateSetter(secondJoin.Item2) : GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = thirdJoin.Item2 != null ? GetOrCreateSetter(thirdJoin.Item2) : GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = fourthJoin.Item2 != null ? GetOrCreateSetter(fourthJoin.Item2) : GetOrCreateSetter(fourthJoin.Item1);
            var fifthSetter = fifthJoin.Item2 != null ? GetOrCreateSetter(fifthJoin.Item2) : GetOrCreateSetter(fifthJoin.Item1);
            var sixthSetter = sixthJoin.Item2 != null ? GetOrCreateSetter(sixthJoin.Item2) : GetOrCreateSetter(sixthJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter);
        }

        private (string, IDictionary<string, object>, string, Action<T, TSecond>, Action<T, TSecond, TThird>, Action<T, TSecond, TThird, TFourth>, Action<T, TSecond, TThird, TFourth, TFifth>, Action<T, TSecond, TThird, TFourth, TFifth, TSixth>, Action<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>) GenerateQueryStatement<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            string tableName,
            string alias,
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate,
            IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> orderings = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> selector = null,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> groupingColumns = null,
            Expression<Func<Grouping<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>, T>> groupingSelector = null,
            bool distinct = false,
            int? skipped = null,
            int? taken = null)
        {
            if (groupingColumns != null && groupingSelector == null) throw new ArgumentException("Must has grouping selection.");

            var aliases = new[] { alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4), GenerateAlias(typeof(TFifth), 5), GenerateAlias(typeof(TSixth), 6), GenerateAlias(typeof(TSeventh), 7) };

            SqlBuilder sql = $@"
SELECT {(distinct ? "DISTINCT " : string.Empty)}{(taken.HasValue && !skipped.HasValue ? string.Concat("TOP(", taken, ") ") : string.Empty)}";

            string splitOn;

            if (groupingSelector != null)
            {
                sql += @"* FROM
(
SELECT ";
                sql += groupingSelector.ToGroupingSelectList(aliases);

                splitOn = string.Empty;
            }
            else if (selector != null)
            {
                sql += selector.ToJoinSelectList(aliases, out splitOn);
            }
            else
            {
                throw new ArgumentException("Must be at least one column selected.");
            }

            sql += $@"
FROM [{tableName}] [{alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item3, sixthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSeventh>(seventhJoin.Item3, seventhJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            var groupingExpression = groupingColumns == null ? string.Empty : groupingColumns.ToGroupingColumns(aliases);

            if (!string.IsNullOrEmpty(groupingExpression))
            {
                sql += @"
GROUP BY ";
                sql += groupingExpression;
            }

            if (groupingSelector != null)
            {
                sql += @"
) [__T__]";
            }

            var orderExpressions = orderings.ToOrderExpressions(groupingSelector != null ? new string[] { } : aliases);

            if (!string.IsNullOrEmpty(orderExpressions))
            {
                sql += @"
ORDER BY ";
                sql += orderExpressions;
            }

            if (skipped.HasValue)
            {
                sql += $@"
OFFSET {skipped.Value} ROWS";

                if (taken.HasValue)
                {
                    sql += $@"
FETCH NEXT {taken.Value} ROWS ONLY";
                }
            }

            sql += ";";

            var secondSetter = secondJoin.Item2 != null ? GetOrCreateSetter(secondJoin.Item2) : GetOrCreateSetter(secondJoin.Item1);
            var thirdSetter = thirdJoin.Item2 != null ? GetOrCreateSetter(thirdJoin.Item2) : GetOrCreateSetter(thirdJoin.Item1);
            var fourthSetter = fourthJoin.Item2 != null ? GetOrCreateSetter(fourthJoin.Item2) : GetOrCreateSetter(fourthJoin.Item1);
            var fifthSetter = fifthJoin.Item2 != null ? GetOrCreateSetter(fifthJoin.Item2) : GetOrCreateSetter(fifthJoin.Item1);
            var sixthSetter = sixthJoin.Item2 != null ? GetOrCreateSetter(sixthJoin.Item2) : GetOrCreateSetter(sixthJoin.Item1);
            var seventhSetter = seventhJoin.Item2 != null ? GetOrCreateSetter(seventhJoin.Item2) : GetOrCreateSetter(seventhJoin.Item1);

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters, splitOn, secondSetter, thirdSetter, fourthSetter, fifthSetter, sixthSetter, seventhSetter);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement(Expression<Func<T, bool>> predicate)
        {
            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(this.alias, parameters, null);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            Expression<Func<T, TSecond, bool>> predicate)
        {
            var aliases = new[] { this.alias, GenerateAlias(typeof(TSecond), 2) };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate)
        {
            var aliases = new[] { this.alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3) };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5),
                              GenerateAlias(typeof(TSixth), 6)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item3, sixthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateCountStatement<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5),
                              GenerateAlias(typeof(TSixth), 6),
                              GenerateAlias(typeof(TSeventh), 7)
                          };

            SqlBuilder sql = $@"
SELECT COUNT(*)
FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item3, sixthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSeventh>(seventhJoin.Item3, seventhJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += ";";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement(Expression<Func<T, bool>> predicate)
        {
            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(this.alias, parameters, null);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement<TSecond>((Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin, Expression<Func<T, TSecond, bool>> predicate)
        {
            var aliases = new[] { this.alias, GenerateAlias(typeof(TSecond), 2) };

            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement<TSecond, TThird>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            Expression<Func<T, TSecond, TThird, bool>> predicate)
        {
            var aliases = new[] { this.alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3) };

            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement<TSecond, TThird, TFourth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias, GenerateAlias(typeof(TSecond), 2), GenerateAlias(typeof(TThird), 3), GenerateAlias(typeof(TFourth), 4)
                          };

            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement<TSecond, TThird, TFourth, TFifth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5)
                          };

            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement<TSecond, TThird, TFourth, TFifth, TSixth>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5),
                              GenerateAlias(typeof(TSixth), 6)
                          };

            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item3, sixthJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, IDictionary<string, object>) GenerateExistsStatement<TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(
            (Expression<Func<T, TSecond>>, Expression<Func<T, List<TSecond>>>, Expression<Func<T, TSecond, bool>>, JoinType) secondJoin,
            (Expression<Func<T, TSecond, TThird>>, Expression<Func<T, TSecond, List<TThird>>>, Expression<Func<T, TSecond, TThird, bool>>, JoinType) thirdJoin,
            (Expression<Func<T, TSecond, TThird, TFourth>>, Expression<Func<T, TSecond, TThird, List<TFourth>>>, Expression<Func<T, TSecond, TThird, TFourth, bool>>, JoinType) fourthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth>>, Expression<Func<T, TSecond, TThird, TFourth, List<TFifth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>>, JoinType) fifthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, List<TSixth>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>>, JoinType) sixthJoin,
            (Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, List<TSeventh>>>, Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>>, JoinType) seventhJoin,
            Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> predicate)
        {
            var aliases = new[]
                          {
                              this.alias,
                              GenerateAlias(typeof(TSecond), 2),
                              GenerateAlias(typeof(TThird), 3),
                              GenerateAlias(typeof(TFourth), 4),
                              GenerateAlias(typeof(TFifth), 5),
                              GenerateAlias(typeof(TSixth), 6),
                              GenerateAlias(typeof(TSeventh), 7)
                          };

            SqlBuilder sql = $@"
SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [{this.tableName}] [{this.alias}] WITH (NOLOCK)";

            var parameters = new Dictionary<string, object>();

            sql += this.GenerateJoinStatement<TSecond>(secondJoin.Item3, secondJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TThird>(thirdJoin.Item3, thirdJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFourth>(fourthJoin.Item3, fourthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TFifth>(fifthJoin.Item3, fifthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSixth>(sixthJoin.Item3, sixthJoin.Item4, aliases, parameters);
            sql += this.GenerateJoinStatement<TSeventh>(seventhJoin.Item3, seventhJoin.Item4, aliases, parameters);

            var searchCondition = predicate == null ? string.Empty : predicate.ToSearchCondition(aliases, parameters);

            if (!string.IsNullOrEmpty(searchCondition))
            {
                sql += @"
WHERE ";
                sql += searchCondition;
            }

            sql += @") THEN 1
        ELSE 0
    END AS BIT);";

            if (!this.IsDirtyRead) sql.Replace(" WITH (NOLOCK)", string.Empty);

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private string GenerateInsertStatement(Expression<Func<T, object>> output = null, Expression<Func<T, bool>> nonexistence = null, IDictionary<string, object> parameters = null)
        {
            var requiredColumns = RequiredColumns.GetOrAdd(
                typeof(T),
                type => type.GetProperties().Where(p => Attribute.IsDefined(p, typeof(RequiredAttribute))).ToArray());

            if (requiredColumns.Length == 0) throw new ArgumentException("There must be at least one [Required] column.");

            var columnList = requiredColumns.ToColumnList(out var valueList);

            SqlBuilder sql;
            
            var tmpTable = output != null ? $"#_{Guid.NewGuid()}" : string.Empty;

            if (output != null)
            {
                var outputSelectList = output.ToOutputSelectList();
                var insertedColumnList = output.ToColumnList().Replace("[", "[INSERTED].[");

                sql = $@"
SELECT
    {outputSelectList} INTO [{tmpTable}]
FROM [{this.tableName}] WITH (NOLOCK)
WHERE 1 = 0;

INSERT INTO [{this.tableName}]({columnList})
OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }
            else
            {
                sql = $@"
INSERT INTO [{this.tableName}]({columnList})";
            }

            if (nonexistence != null)
            {
                var nonexistenceCondition = nonexistence.ToSearchCondition(this.alias, parameters, null);

                sql += $@"
    SELECT {valueList}
    WHERE NOT EXISTS (SELECT
                1
            FROM [{this.tableName}] [{this.alias}]
            WHERE {nonexistenceCondition});";
            }
            else
            {
                sql += $@"
    SELECT {valueList};";
            }

            if (output != null)
            {
                sql += $@"

SELECT
    *
FROM [{tmpTable}]

DROP TABLE [{tmpTable}];";
            }

            this.OutputSql?.Invoke(sql, null);

            return sql;
        }

        private (string, IDictionary<string, object>) GenerateInsertStatement(Expression<Func<T>> setter, bool outParameters, Expression<Func<T, object>> output = null, Expression<Func<T, bool>> nonexistence = null)
        {
            string valueList;
            IDictionary<string, object> parameters = null;

            var columnList = outParameters ? setter.ToColumnList(out valueList, out parameters) : setter.ToColumnList(out valueList);

            SqlBuilder sql;

            var tmpTable = output != null ? $"#_{Guid.NewGuid()}" : string.Empty;

            if (output != null)
            {
                var outputSelectList = output.ToOutputSelectList();
                var insertedColumnList = output.ToColumnList().Replace("[", "[INSERTED].[");

                sql = $@"
SELECT
    {outputSelectList} INTO [{tmpTable}]
FROM [{this.tableName}] WITH (NOLOCK)
WHERE 1 = 0;

INSERT INTO [{this.tableName}]({columnList})
OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }
            else
            {
                sql = $@"
INSERT INTO [{this.tableName}]({columnList})";
            }

            if (nonexistence != null)
            {
                var nonexistenceCondition = outParameters ? nonexistence.ToSearchCondition(this.alias, parameters, null) : nonexistence.ToSearchCondition(this.alias);

                sql += $@"
    SELECT {valueList}
    WHERE NOT EXISTS (SELECT
                1
            FROM [{this.tableName}] [{this.alias}]
            WHERE {nonexistenceCondition});";
            }
            else
            {
                sql += $@"
    SELECT {valueList};";
            }

            if (output != null)
            {
                sql += $@"

SELECT
    *
FROM [{tmpTable}]

DROP TABLE [{tmpTable}]";
            }

            this.OutputSql?.Invoke(sql, parameters);

            return (sql, parameters);
        }

        private (string, string, DataTable) GenerateBulkInsertStatement(IEnumerable<T> values, Expression<Func<T, object>> output = null, Expression<Func<T, bool>> nonexistence = null)
        {
            var userDefinedColumnNameMap = nonexistence != null ? new Dictionary<string, string>() : null;

            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values, userDefinedColumnNameMap);

            var requiredColumns = RequiredColumns.GetOrAdd(
                typeof(T),
                type => type.GetProperties().Where(p => Attribute.IsDefined(p, typeof(RequiredAttribute))).ToArray());

            if (requiredColumns.Length == 0) throw new ArgumentException("There must be at least one [Required] column.");

            var columnList = requiredColumns.ToColumnList(out _);

            SqlBuilder sql;

            var tmpTable = output != null ? $"#_{Guid.NewGuid()}" : string.Empty;

            if (output != null)
            {
                var outputSelectList = output.ToOutputSelectList();
                var insertedColumnList = output.ToColumnList().Replace("[", "[INSERTED].[");

                sql = $@"
SELECT
    {outputSelectList} INTO [{tmpTable}]
FROM [{this.tableName}] WITH (NOLOCK)
WHERE 1 = 0;

INSERT INTO [{this.tableName}]({columnList})
OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }
            else
            {
                sql = $@"
INSERT INTO [{this.tableName}]({columnList})";
            }

            if (nonexistence != null)
            {
                var nonexistenceCondition = nonexistence.ToSearchCondition(this.alias, userDefinedColumnNameMap);

                sql += $@"
    SELECT {ColumnRegex.Replace(columnList, "tvp.$0")}
    FROM @TableVariable tvp
    WHERE NOT EXISTS (SELECT
                1
            FROM [{this.tableName}] [{this.alias}]
            WHERE {ParameterRegex.Replace(nonexistenceCondition, "tvp.[$1]")});";
            }
            else
            {
                sql += $@"
    SELECT {ColumnRegex.Replace(columnList, "tvp.$0")}
    FROM @TableVariable tvp;";
            }

            if (output != null)
            {
                sql += $@"

SELECT
    *
FROM [{tmpTable}]

DROP TABLE [{tmpTable}]";
            }

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, string, DataTable) GenerateBulkInsertStatement(Expression<Func<T>> setterTemplate, IEnumerable<T> values, Expression<Func<T, object>> output = null, Expression<Func<T, bool>> nonexistence = null)
        {
            var userDefinedColumnNameMap = nonexistence != null ? new Dictionary<string, string>() : null;

            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values, userDefinedColumnNameMap);

            var columnList = setterTemplate.ToColumnList(out _);

            SqlBuilder sql;

            var tmpTable = output != null ? $"#_{Guid.NewGuid()}" : string.Empty;

            if (output != null)
            {
                var outputSelectList = output.ToOutputSelectList();
                var insertedColumnList = output.ToColumnList().Replace("[", "[INSERTED].[");

                sql = $@"
SELECT
    {outputSelectList} INTO [{tmpTable}]
FROM [{this.tableName}] WITH (NOLOCK)
WHERE 1 = 0;

INSERT INTO [{this.tableName}]({columnList})
OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }
            else
            {
                sql = $@"
INSERT INTO [{this.tableName}]({columnList})";
            }

            if (nonexistence != null)
            {
                var nonexistenceCondition = nonexistence.ToSearchCondition(this.alias, userDefinedColumnNameMap);

                sql += $@"
    SELECT {ColumnRegex.Replace(columnList, "tvp.$0")}
    FROM @TableVariable tvp
    WHERE NOT EXISTS (SELECT
                1
            FROM [{this.tableName}] [{this.alias}]
            WHERE {ParameterRegex.Replace(nonexistenceCondition, "tvp.[$1]")});";
            }
            else
            {
                sql += $@"
    SELECT {ColumnRegex.Replace(columnList, "tvp.$0")}
    FROM @TableVariable tvp;";
            }

            if (output != null)
            {
                sql += $@"

SELECT
    *
FROM [{tmpTable}]

DROP TABLE [{tmpTable}]";
            }

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, IDictionary<string, object>) GenerateUpdateStatement(Expression<Func<T, bool>> predicate, Expression<Func<T>> setter, bool outParameters)
        {
            IDictionary<string, object> parameters = null;

            SqlBuilder sql = $@"
UPDATE [{this.tableName}]
SET ";
            sql += outParameters ? setter.ToSetStatements(out parameters) : setter.ToSetStatements();
            sql += @"
WHERE ";
            sql += outParameters ? predicate.ToSearchCondition(parameters) : predicate.ToSearchCondition();
            sql += ";";

            this.OutputSql?.Invoke(sql, null);

            return (sql, parameters);
        }

        private (string, string, DataTable) GenerateBulkUpdateStatement(Expression<Func<T, bool>> predicateTemplate, Expression<Func<T>> setterTemplate, IEnumerable<T> values)
        {
            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values, null);

            var columnList = setterTemplate.ToColumnList(out _);
            var searchCondition = predicateTemplate.ToSearchCondition();

            var sql = $@"
UPDATE [{this.tableName}]
SET {ColumnRegex.Replace(columnList, "$0 = tvp.$0")}
FROM [{this.tableName}] t
INNER JOIN @TableVariable tvp
    ON {ColumnValueRegex.Replace(searchCondition, "t.$1 = tvp.$1")};";

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, IDictionary<string, object>) GenerateUpsertStatement(
            Expression<Func<T, bool>> predicate,
            Expression<Func<T>> setter,
            bool outParameters,
            Expression<Func<T, object>> output = null)
        {
            IDictionary<string, object> parameters = null;

            SqlBuilder sql;

            var tmpTable = output != null ? $"#_{Guid.NewGuid()}" : string.Empty;

            var insertedColumnList = string.Empty;

            if (output != null)
            {
                var outputSelectList = output.ToOutputSelectList();
                insertedColumnList = output.ToColumnList().Replace("[", "[INSERTED].[");

                sql = $@"
SELECT
    {outputSelectList} INTO [{tmpTable}]
FROM [{this.tableName}] WITH (NOLOCK)
WHERE 1 = 0;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
";
            }
            else
            {
                sql = @"
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
";
            }

            sql += $@"
UPDATE [{this.tableName}] WITH (ROWLOCK)
SET ";
            sql += outParameters ? setter.ToSetStatements(out parameters) : setter.ToSetStatements();

            if (output != null)
            {
                sql += $@"
OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }

            sql += @"
WHERE ";
            sql += outParameters ? predicate.ToSearchCondition(parameters) : predicate.ToSearchCondition();

            var (columnList, valueList) = ResolveColumnList(sql);

            sql.Append("\r\n");
            sql += $@"
IF @@rowcount = 0
    BEGIN
        INSERT INTO [{this.tableName}]({columnList})";

            if (output != null)
            {
                sql += $@"
        OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }

            sql += $@"
            SELECT {valueList}
    END;";

            if (output != null)
            {
                sql += $@"

SELECT
    *
FROM [{tmpTable}]

DROP TABLE [{tmpTable}]";
            }

            this.OutputSql?.Invoke(sql, null);

            return (sql, parameters);
        }

        private (string, string, DataTable) GenerateBulkUpsertStatement(
            Expression<Func<T, bool>> predicateTemplate,
            Expression<Func<T>> setterTemplate,
            IEnumerable<T> values,
            Expression<Func<T, object>> output = null)
        {
            var (tableType, tableVariable) = this.ConvertToTableValuedParameters(values, null);

            var columnList = setterTemplate.ToColumnList(out _);
            var searchCondition = predicateTemplate.ToSearchCondition();

            SqlBuilder sql = string.Empty;

            var tmpTable = output != null ? $"#_{Guid.NewGuid()}" : string.Empty;

            var insertedColumnList = string.Empty;

            if (output != null)
            {
                var outputSelectList = output.ToOutputSelectList();
                insertedColumnList = output.ToColumnList().Replace("[", "[INSERTED].[");

                sql = $@"
SELECT
    {outputSelectList} INTO [{tmpTable}]
FROM [{this.tableName}] WITH (NOLOCK)
WHERE 1 = 0;
";
            }

            sql += $@"
UPDATE [{this.tableName}]
SET {ColumnRegex.Replace(columnList, "$0 = tvp.$0")}";

            if (output != null)
            {
                sql += $@"
OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }

            sql += $@"
FROM [{this.tableName}] t
INNER JOIN @TableVariable tvp
    ON {ColumnValueRegex.Replace(searchCondition, "t.$1 = tvp.$1")}";

            (columnList, _) = ResolveColumnList(sql);

            sql.Append("\r\n");
            sql += $@"
INSERT INTO [{this.tableName}]({columnList})";

            if (output != null)
            {
                sql += $@"
OUTPUT {insertedColumnList} INTO [{tmpTable}]";
            }

            sql += $@"
    SELECT {ColumnRegex.Replace(columnList, "tvp.$0")}
    FROM @TableVariable tvp
    WHERE NOT EXISTS (SELECT
                1
            FROM [{this.tableName}] t WITH (NOLOCK)
            WHERE {ColumnValueRegex.Replace(searchCondition, "t.$1 = tvp.$1")});";

            if (output != null)
            {
                sql += $@"

SELECT
    *
FROM [{tmpTable}]

DROP TABLE [{tmpTable}]";
            }

            this.OutputSql?.Invoke(sql, null);

            return (sql, tableType, tableVariable);
        }

        private (string, IDictionary<string, object>) GenerateDeleteStatement(Expression<Func<T, bool>> predicate)
        {
            SqlBuilder sql = $@"
DELETE FROM [{this.tableName}]
WHERE ";
            sql += predicate.ToSearchCondition(out var parameters);

            this.OutputSql?.Invoke(sql, null);

            return (sql, parameters);
        }

        private string GenerateJoinStatement<TRight>(LambdaExpression condition, JoinType joinType, string[] aliases, IDictionary<string, object> parameters)
        {
            if (condition == null)
            {
                throw new ArgumentException("Must have join condition.");
            }

            var server = ServerRegex.Match(this.connectionString).Groups[2].Value.Trim();
            var database = DatabaseRegex.Match(this.connectionString).Groups[2].Value.Trim();
            var rightServer = server;
            var rightDatabase = database;
            var rightSchema = "dbo";

            foreach (var connectionStringAttr in typeof(TRight).GetCustomAttributes<ConnectionStringAttribute>())
            {
                var rightConnectionString = SqlServerDataAccessFactory.Instance.GetConnectionString(connectionStringAttr.ConnectionString);

                rightServer = ServerRegex.Match(rightConnectionString).Groups[2].Value.Trim();
                rightDatabase = DatabaseRegex.Match(rightConnectionString).Groups[2].Value.Trim();
                rightSchema = connectionStringAttr.Schema;

                if (string.Equals(server, rightServer, StringComparison.CurrentCultureIgnoreCase)) break;
            }

            if (!string.Equals(server, rightServer, StringComparison.CurrentCultureIgnoreCase))
            {
                throw new ArgumentException("Table is not in the same database server.");
            }

            switch (joinType)
            {
                case JoinType.Inner:
                    return string.Equals(database, rightDatabase, StringComparison.CurrentCultureIgnoreCase)
                               ? string.Concat("\r\n", condition.ToInnerJoin<TRight>(aliases, null, null, parameters))
                               : string.Concat("\r\n", condition.ToInnerJoin<TRight>(aliases, rightDatabase, rightSchema, parameters));

                case JoinType.Left:
                    return string.Equals(database, rightDatabase, StringComparison.CurrentCultureIgnoreCase)
                               ? string.Concat("\r\n", condition.ToLeftJoin<TRight>(aliases, null, null, parameters))
                               : string.Concat("\r\n", condition.ToLeftJoin<TRight>(aliases, rightDatabase, rightSchema, parameters));

                default: throw new ArgumentOutOfRangeException(nameof(joinType), "Unsupported join type.");
            }
        }

        private (string, DataTable) ConvertToTableValuedParameters(IEnumerable<T> values, IDictionary<string, string> userDefinedColumnNameMap)
        {
            var userDefinedAttribute = typeof(T).GetCustomAttribute<UserDefinedAttribute>(true);

            if (userDefinedAttribute == null) throw new ArgumentException("Must has UserDefinedAttribute.");

            var tableType = userDefinedAttribute.TableType;

            var columns = SqlServerDataAccessFactory.Instance.GetUserDefinedTable(tableType);

            if (string.IsNullOrEmpty(tableType) || columns == null)
            {
                throw new ArgumentException("Must configure user-defined table type.");
            }

            var dataTable = new DataTable();

            dataTable.Columns.AddRange(columns.ToArray());

            var properties = typeof(T).GetProperties().ToDictionary(x => x.Name, x => x);

            var propertyMap = new Dictionary<string, PropertyInfo>();

            foreach (var dataColumn in columns)
            {
                if (!properties.TryGetValue(dataColumn.ColumnName, out var property))
                {
                    property = properties.Values.Single(p => p.GetCustomAttribute<ColumnAttribute>()?.Name == dataColumn.ColumnName);
                }

                propertyMap[dataColumn.ColumnName] = property;

                if (userDefinedColumnNameMap != null)
                {
                    userDefinedColumnNameMap[property.Name] = dataColumn.ColumnName;
                }
            }

            foreach (var value in values)
            {
                var dataRow = dataTable.NewRow();

                foreach (var dataColumn in columns)
                {
                    dataRow[dataColumn.ColumnName] = propertyMap[dataColumn.ColumnName].GetValue(value);
                }

                dataTable.Rows.Add(dataRow);
            }

            return (tableType, dataTable);
        }
    }
}