﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Dapper;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class StatementExtension
    {
        private static readonly HashSet<Type> NumericTypes = new HashSet<Type>
                                                             {
                                                                 typeof(bool),
                                                                 typeof(byte),
                                                                 typeof(sbyte),
                                                                 typeof(short),
                                                                 typeof(ushort),
                                                                 typeof(int),
                                                                 typeof(uint),
                                                                 typeof(long),
                                                                 typeof(ulong),
                                                                 typeof(float),
                                                                 typeof(double),
                                                                 typeof(decimal),
                                                             };

        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> AllColumns = new ConcurrentDictionary<Type, PropertyInfo[]>();

        public static string ToSelectList(this PropertyInfo[] me)
        {
            return ToSelectList(me, string.Empty);
        }

        public static string ToSelectList(this PropertyInfo[] me, string alias)
        {
            if (me == null || me.Length == 0) throw new ArgumentException($"'{nameof(me)}' can not be null or empty.");

            alias = string.IsNullOrEmpty(alias) ? alias : string.Concat("[", alias, "]");

            var sb = new StringBuilder();

            foreach (var propertyInfo in me)
            {
                if (Attribute.IsDefined(propertyInfo, typeof(NotMappedAttribute))) continue;

                var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name;

                sb.AliasAppend(string.IsNullOrEmpty(columnName) ? $"[{propertyInfo.Name}], " : $"[{columnName}] AS [{propertyInfo.Name}], ", alias);
            }

            sb.Remove(sb.Length - 2, 2);

            return sb.ToString();
        }

        public static string ToSelectList<T>(this Expression<Func<T, object>> me)
        {
            return ToSelectList(me, string.Empty);
        }

        public static string ToSelectList<T>(this Expression<Func<T, object>> me, string alias)
        {
            var memberExprs = GetMemberExpressions(me.Body);

            alias = string.IsNullOrEmpty(alias) ? alias : string.Concat("[", alias, "]");

            var sb = new StringBuilder();

            foreach (var memberExpr in memberExprs)
            {
                if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute))) continue;

                var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name;

                sb.AliasAppend(string.IsNullOrEmpty(columnName) ? $"[{memberExpr.Member.Name}], " : $"[{columnName}] AS [{memberExpr.Member.Name}], ", alias);
            }

            sb.Remove(sb.Length - 2, 2);

            return sb.ToString();
        }

        public static string ToOutputSelectList<T>(this Expression<Func<T, object>> me)
        {
            var memberExprs = GetMemberExpressions(me.Body);

            var sb = new StringBuilder();

            foreach (var memberExpr in memberExprs)
            {
                if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute))) continue;

                sb.Append(GenerateOutputColumnStatement(memberExpr.Member));
            }

            sb.Remove(sb.Length - 2, 2);

            return sb.ToString();

            static string GenerateOutputColumnStatement(MemberInfo member)
            {
                // For IDENTITY Column
                var memberType = (member as PropertyInfo)?.PropertyType;
                if (memberType == typeof(int)) return $"CAST(0 AS INT) AS [{member.Name}], ";
                if (memberType == typeof(long)) return $"CAST(0 AS BIGINT) AS [{member.Name}], ";

                var columnAttribute = member.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name;

                return string.IsNullOrEmpty(columnName) ? $"[{member.Name}], " : $"[{columnName}] AS [{member.Name}], ";
            }
        }

        public static string ToJoinSelectList(this LambdaExpression lambdaExpr, string[] aliases, out string splitOn)
        {
            var memberExprs = GetMemberExpressions(lambdaExpr.Body);

            var splitOnList = new List<string>();
            var aliasMap = GenerateAliasMap(lambdaExpr.Parameters, aliases);
            var selectorContainers = lambdaExpr.Parameters.ToDictionary(x => x.Name, x => new List<string>());

            foreach (var memberExpr in memberExprs)
            {
                if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute))) continue;

                var parameterExpr = (ParameterExpression)memberExpr.Expression;

                var alias = aliasMap[parameterExpr.Name];
                var selectorContainer = selectorContainers[parameterExpr.Name];
                var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name;

                var statement = string.IsNullOrEmpty(columnName)
                                    ? $"[{memberExpr.Member.Name}]"
                                    : $"[{columnName}] AS [{memberExpr.Member.Name}]";

                if (!string.IsNullOrEmpty(alias)) statement = string.Concat($"{alias}.", statement);

                if (!selectorContainer.Any()) splitOnList.Add(memberExpr.Member.Name);

                selectorContainer.Add(statement);
            }

            if (splitOnList.Count < selectorContainers.Count) throw new InvalidOperationException("Selected columns must cover all joined tables.");

            splitOn = string.Join(",", splitOnList.Skip(1));

            return string.Join(", ", selectorContainers.SelectMany(x => x.Value));
        }

        public static string ToGroupingColumns(this LambdaExpression lambdaExpr, string[] aliases)
        {
            var memberExprs = GetMemberExpressions(lambdaExpr.Body);

            var aliasMap = GenerateAliasMap(lambdaExpr.Parameters, aliases);
            var selectorContainers = lambdaExpr.Parameters.ToDictionary(x => x.Name, x => new List<string>());

            foreach (var memberExpr in memberExprs)
            {
                if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute))) continue;

                var parameterExpr = (ParameterExpression)memberExpr.Expression;

                var alias = aliases.Length == 1 ? aliases[0] : aliasMap[parameterExpr.Name];

                var selectorContainer = selectorContainers[parameterExpr.Name];
                var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name;

                var statement = string.IsNullOrEmpty(columnName) ? $"[{memberExpr.Member.Name}]" : $"[{columnName}]";

                if (!string.IsNullOrEmpty(alias)) statement = string.Concat($"{alias}.", statement);

                selectorContainer.Add(statement);
            }

            return string.Join(", ", selectorContainers.SelectMany(x => x.Value));
        }

        public static string ToGroupingSelectList(this LambdaExpression lambdaExpr, string[] aliases)
        {
            if (!(lambdaExpr.Body is MemberInitExpression memberInitExpr))
            {
                throw new ArgumentException("Grouping selector must be a MemberInitExpression.");
            }

            var memberAssignments = memberInitExpr.Bindings.Select(x => (MemberAssignment)x);
            var selectorContainer = new List<string>();

            foreach (var memberAssignment in memberAssignments)
            {
                if (Attribute.IsDefined(memberAssignment.Member, typeof(NotMappedAttribute))) continue;

                if (memberAssignment.Expression.NodeType == ExpressionType.Convert)
                {
                    throw new InvalidCastException("Grouping selector assignment type must match.");
                }

                if (!(memberAssignment.Expression is MethodCallExpression methodCallExpr))
                {
                    throw new ArgumentException("Grouping selector assignment must be a MethodCallExpression.");
                }

                var methodFullName = methodCallExpr.Method.GetFullName();
                var statementBuilder = new StringBuilder();

                if (methodCallExpr.Arguments.Count > 0)
                {
                    var selectExpr = (methodCallExpr.Arguments[0] as UnaryExpression).Operand as LambdaExpression;
                    var memberExpr = selectExpr.Body as MemberExpression;
                    var parameterExpr = memberExpr.Expression as ParameterExpression;

                    var parameterIndex = selectExpr.Parameters.FindIndex(p => p.Name == parameterExpr.Name);

                    var alias = aliases[parameterIndex];

                    var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                    var columnName = columnAttribute?.Name ?? memberExpr.Member.Name;
                    var selectColumn = $"[{alias}].[{columnName}]";

                    if (methodFullName.EndsWith(".Select"))
                    {
                        statementBuilder.Append(selectColumn);
                    }
                    else if (methodFullName.EndsWith(".Avg"))
                    {
                        statementBuilder.Append("AVG(CAST(");
                        statementBuilder.Append(selectColumn);
                        statementBuilder.Append(" AS DECIMAL(38, 6)))");
                    }
                    else
                    {
                        if (methodFullName.EndsWith(".Count"))
                        {
                            statementBuilder.Append("COUNT(");
                        }
                        else if (methodFullName.EndsWith(".Max"))
                        {
                            statementBuilder.Append("MAX(");
                        }
                        else if (methodFullName.EndsWith(".Min"))
                        {
                            statementBuilder.Append("MIN(");
                        }
                        else if (methodFullName.EndsWith(".Sum"))
                        {
                            statementBuilder.Append("SUM(");
                        }

                        statementBuilder.Append(selectColumn);
                        statementBuilder.Append(")");
                    }
                }
                else
                {
                    if (methodFullName.EndsWith(".Count"))
                    {
                        statementBuilder.Append("COUNT(*)");
                    }
                }

                statementBuilder.Append($" AS [{memberAssignment.Member.Name}]");

                selectorContainer.Add(statementBuilder.ToString());
            }

            return string.Join(", ", selectorContainer);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me)
        {
            return ToSearchCondition(me, string.Empty, null, null, null);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me, out List<PropertyInfo> involvedMembers)
        {
            involvedMembers = new List<PropertyInfo>();

            return ToSearchCondition(me, string.Empty, null, null, involvedMembers);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me, string alias)
        {
            return ToSearchCondition(me, alias, null, null, null);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me, string alias, IDictionary<string, string> parameterNames)
        {
            return ToSearchCondition(me, alias, null, parameterNames, null);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me, out IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, string.Empty, out parameters);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me, string alias, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, alias, parameters, null, null);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me, IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, string.Empty, parameters, null, null);
        }

        public static string ToSearchCondition<T>(this Expression<Func<T, bool>> me, string alias, IDictionary<string, object> parameters, IDictionary<string, string> parameterNames, List<PropertyInfo> involvedMembers)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, new[] { alias });

            var sb = new StringBuilder();

            ParseCondition(me.Body, aliasMap, sb, parameters, parameterNames, involvedMembers);

            return sb.ToString();
        }

        public static string ToSearchCondition<T, TSecond>(this Expression<Func<T, TSecond, bool>> me, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond>(this Expression<Func<T, TSecond, bool>> me, string[] aliases, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, aliases, parameters);
        }

        public static string ToSearchCondition<T, TSecond>(this Expression<Func<T, TSecond, bool>> me, IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond>(this Expression<Func<T, TSecond, bool>> me, string[] aliases)
        {
            return ToSearchCondition(me, aliases, null);
        }

        public static string ToSearchCondition<T, TSecond>(this Expression<Func<T, TSecond, bool>> me, string[] aliases, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var sb = new StringBuilder();

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToSearchCondition<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, bool>> me, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, bool>> me, string[] aliases, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, aliases, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, bool>> me, IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, bool>> me, string[] aliases)
        {
            return ToSearchCondition(me, aliases, null);
        }

        public static string ToSearchCondition<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, bool>> me, string[] aliases, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var sb = new StringBuilder();

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, bool>> me, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, bool>> me, string[] aliases, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, aliases, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, bool>> me, IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, bool>> me, string[] aliases)
        {
            return ToSearchCondition(me, aliases, null);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, bool>> me, string[] aliases, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var sb = new StringBuilder();

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> me, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> me, string[] aliases, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, aliases, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> me, IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> me, string[] aliases)
        {
            return ToSearchCondition(me, aliases, null);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, bool>> me, string[] aliases, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var sb = new StringBuilder();

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> me, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> me, string[] aliases, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, aliases, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> me, IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> me, string[] aliases)
        {
            return ToSearchCondition(me, aliases, null);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, bool>> me, string[] aliases, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var sb = new StringBuilder();

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> me, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> me, string[] aliases, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSearchCondition(me, aliases, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> me, IDictionary<string, object> parameters)
        {
            return ToSearchCondition(me, new string[] { }, parameters);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> me, string[] aliases)
        {
            return ToSearchCondition(me, aliases, null);
        }

        public static string ToSearchCondition<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, bool>> me, string[] aliases, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var sb = new StringBuilder();

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToInnerJoin<TRight>(this LambdaExpression me, string database, string schema, IDictionary<string, object> parameters)
        {
            return ToInnerJoin<TRight>(me, new string[] { }, database, schema, parameters);
        }

        public static string ToInnerJoin<TRight>(this LambdaExpression me, string[] aliases, string database, string schema, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var rightTable = typeof(TRight).GetCustomAttribute<TableAttribute>()?.Name ?? typeof(TRight).Name;
            var rightTableAlias = aliasMap[me.Parameters.Last().Name];

            var sb = new StringBuilder();

            if (!string.IsNullOrEmpty(database) && !string.IsNullOrEmpty(schema))
            {
                sb.Append($"INNER JOIN [{database}].[{schema}].[{rightTable}]");
            }
            else
            {
                sb.Append($"INNER JOIN [{rightTable}]");
            }

            if (!string.IsNullOrEmpty(rightTableAlias)) sb.Append($" {rightTableAlias}");

            sb.Append(" WITH (NOLOCK) ON ");

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToLeftJoin<TRight>(this LambdaExpression me, string database, string schema, IDictionary<string, object> parameters)
        {
            return ToLeftJoin<TRight>(me, new string[] { }, database, schema, parameters);
        }

        public static string ToLeftJoin<TRight>(this LambdaExpression me, string[] aliases, string database, string schema, IDictionary<string, object> parameters)
        {
            var aliasMap = GenerateAliasMap(me.Parameters, aliases);

            var rightTable = typeof(TRight).GetCustomAttribute<TableAttribute>()?.Name ?? typeof(TRight).Name;
            var rightTableAlias = aliasMap[me.Parameters.Last().Name];

            var sb = new StringBuilder();

            if (!string.IsNullOrEmpty(database) && !string.IsNullOrEmpty(schema))
            {
                sb.Append($"LEFT JOIN [{database}].[{schema}].[{rightTable}]");
            }
            else
            {
                sb.Append($"LEFT JOIN [{rightTable}]");
            }

            if (!string.IsNullOrEmpty(rightTableAlias)) sb.Append($" {rightTableAlias}");

            sb.Append(" WITH (NOLOCK) ON ");

            ParseCondition(me.Body, aliasMap, sb, parameters, null, null);

            return sb.ToString();
        }

        public static string ToColumnList<T>(this Expression<Func<T, object>> me)
        {
            var memberExprs = GetMemberExpressions(me.Body);

            var sb = new StringBuilder();

            foreach (var memberExpr in memberExprs)
            {
                if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute))) continue;

                var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name;

                sb.Append(string.IsNullOrEmpty(columnName) ? $"[{memberExpr.Member.Name}], " : $"[{columnName}], ");
            }

            sb.Remove(sb.Length - 2, 2);

            return sb.ToString();
        }

        public static string ToColumnList(this PropertyInfo[] me, out string valueList)
        {
            if (me == null || me.Length == 0) throw new ArgumentException($"'{nameof(me)}' can not be null or empty.");

            var columnListBuilder = new StringBuilder();
            var valueListBuilder = new StringBuilder();

            foreach (var propertyInfo in me)
            {
                if (Attribute.IsDefined(propertyInfo, typeof(NotMappedAttribute)))
                {
                    throw new ArgumentException("Member can not applied [NotMapped].");
                }

                var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();
                var parameterName = propertyInfo.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var parameterType = propertyInfo.PropertyType;

                columnListBuilder.Append($"[{columnName}], ");
                valueListBuilder.Append($"{GenerateParameterStatement(parameterName, parameterType, null, null)}, ");
            }

            columnListBuilder.Remove(columnListBuilder.Length - 2, 2);
            valueListBuilder.Remove(valueListBuilder.Length - 2, 2);

            valueList = valueListBuilder.ToString();

            return columnListBuilder.ToString();
        }

        public static string ToColumnList<T>(this Expression<Func<T>> me, out string valueList)
        {
            return ToColumnList(me, out valueList, null);
        }

        public static string ToColumnList<T>(this Expression<Func<T>> me, out string valueList, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToColumnList(me, out valueList, parameters);
        }

        public static string ToColumnList<T>(this Expression<Func<T>> me, out string valueList, IDictionary<string, object> parameters)
        {
            if (!(me.Body is MemberInitExpression memberInitExpr)) throw new ArgumentException("Must be member initializer.");

            var columnListBuilder = new StringBuilder();
            var valueListBuilder = new StringBuilder();

            foreach (var binding in memberInitExpr.Bindings)
            {
                if (!(binding is MemberAssignment memberAssignment)) throw new ArgumentException("Must be member assignment.");

                if (Attribute.IsDefined(memberAssignment.Member, typeof(NotMappedAttribute)))
                {
                    throw new ArgumentException("Member can not applied [NotMapped].");
                }

                var columnAttribute = memberAssignment.Member.GetCustomAttribute<ColumnAttribute>();
                var parameterName = memberAssignment.Member.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var parameterType = ((PropertyInfo)memberAssignment.Member).PropertyType;

                if (parameters != null)
                {
                    SetParameter(memberAssignment.Member, ExtractConstant(memberAssignment.Expression), columnAttribute, parameters, null, out parameterName);
                }

                columnListBuilder.Append($"[{columnName}], ");
                valueListBuilder.Append($"{GenerateParameterStatement(parameterName, parameterType, parameters, null)}, ");
            }

            columnListBuilder.Remove(columnListBuilder.Length - 2, 2);
            valueListBuilder.Remove(valueListBuilder.Length - 2, 2);

            valueList = valueListBuilder.ToString();

            return columnListBuilder.ToString();
        }

        public static string ToColumnDefinitions(this PropertyInfo[] me, out List<UserDefinedField> fields)
        {
            if (me == null || me.Length == 0) throw new ArgumentException($"'{nameof(me)}' can not be null or empty.");

            fields = new List<UserDefinedField>();

            var columnDefinitionsBuilder = new StringBuilder();

            foreach (var propertyInfo in me)
            {
                if (Attribute.IsDefined(propertyInfo, typeof(NotMappedAttribute)))
                {
                    throw new ArgumentException("Member can not applied [NotMapped].");
                }

                var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();
                var parameterName = propertyInfo.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var parameterType = propertyInfo.PropertyType;

                if (fields.Any(f => f.Property.PropertyType.FullName == parameterType.FullName)) continue;

                columnDefinitionsBuilder.Append($"[{columnName}] {MapSqlType(parameterType, columnAttribute)}, ");
                fields.Add(new UserDefinedField(propertyInfo, new DataColumn(columnName, parameterType)));
            }

            columnDefinitionsBuilder.Remove(columnDefinitionsBuilder.Length - 2, 2);

            return columnDefinitionsBuilder.ToString();
        }

        public static string ToColumnDefinitions<T>(this Expression<Func<T>> me, out List<UserDefinedField> fields)
        {
            if (!(me.Body is MemberInitExpression memberInitExpr)) throw new ArgumentException("Must be member initializer.");

            fields = new List<UserDefinedField>();

            var columnDefinitionsBuilder = new StringBuilder();

            foreach (var binding in memberInitExpr.Bindings)
            {
                if (!(binding is MemberAssignment memberAssignment)) throw new ArgumentException("Must be member assignment.");

                if (Attribute.IsDefined(memberAssignment.Member, typeof(NotMappedAttribute)))
                {
                    throw new ArgumentException("Member can not applied [NotMapped].");
                }

                var columnAttribute = memberAssignment.Member.GetCustomAttribute<ColumnAttribute>();
                var parameterName = memberAssignment.Member.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var parameter = (PropertyInfo)memberAssignment.Member;
                var parameterType = parameter.PropertyType;

                if (fields.Any(f => f.Property.PropertyType.FullName == parameterType.FullName)) continue;

                columnDefinitionsBuilder.Append($"[{columnName}] {MapSqlType(parameterType, columnAttribute)}, ");
                fields.Add(new UserDefinedField(parameter, new DataColumn(columnName, parameterType)));
            }

            columnDefinitionsBuilder.Remove(columnDefinitionsBuilder.Length - 2, 2);

            return columnDefinitionsBuilder.ToString();
        }

        public static string ToColumnDefinitions<T>(this Expression<Func<T>> me, IEnumerable<PropertyInfo> additionalMembers, out List<UserDefinedField> fields)
        {
            if (!(me.Body is MemberInitExpression memberInitExpr)) throw new ArgumentException("Must be member initializer.");

            fields = new List<UserDefinedField>();

            var columnDefinitionsBuilder = new StringBuilder();

            foreach (var binding in memberInitExpr.Bindings)
            {
                if (!(binding is MemberAssignment memberAssignment)) throw new ArgumentException("Must be member assignment.");

                if (Attribute.IsDefined(memberAssignment.Member, typeof(NotMappedAttribute)))
                {
                    throw new ArgumentException("Member can not applied [NotMapped].");
                }
                
                var columnAttribute = memberAssignment.Member.GetCustomAttribute<ColumnAttribute>();
                var parameterName = memberAssignment.Member.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var parameter = (PropertyInfo)memberAssignment.Member;
                var parameterType = parameter.PropertyType;

                if (fields.Any(f => f.Property.GetFullName() == parameter.GetFullName())) continue;

                columnDefinitionsBuilder.Append($"[{columnName}] {MapSqlType(parameterType, columnAttribute)}, ");
                fields.Add(new UserDefinedField(parameter, new DataColumn(columnName, parameterType)));

                additionalMembers = additionalMembers.Where(x => x.GetFullName() != parameter.GetFullName());
            }

            foreach (var additionalMember in additionalMembers)
            {
                if (Attribute.IsDefined(additionalMember, typeof(NotMappedAttribute)))
                {
                    throw new ArgumentException("Member can not applied [NotMapped].");
                }

                var columnAttribute = additionalMember.GetCustomAttribute<ColumnAttribute>();
                var parameterName = additionalMember.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var parameterType = additionalMember.PropertyType;

                if (fields.Any(f => f.Property.GetFullName() == additionalMember.GetFullName())) continue;

                columnDefinitionsBuilder.Append($"[{columnName}] {MapSqlType(parameterType, columnAttribute)}, ");
                fields.Add(new UserDefinedField(additionalMember, new DataColumn(columnName, parameterType)));
            }

            columnDefinitionsBuilder.Remove(columnDefinitionsBuilder.Length - 2, 2);

            return columnDefinitionsBuilder.ToString();
        }

        public static string ToSetStatements<T>(this Expression<Func<T>> me)
        {
            return ToSetStatements(me, string.Empty, null);
        }

        public static string ToSetStatements<T>(this Expression<Func<T>> me, string alias)
        {
            return ToSetStatements(me, alias, null);
        }

        public static string ToSetStatements<T>(this Expression<Func<T>> me, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSetStatements(me, string.Empty, parameters);
        }

        public static string ToSetStatements<T>(this Expression<Func<T>> me, string alias, out IDictionary<string, object> parameters)
        {
            parameters = new Dictionary<string, object>();

            return ToSetStatements(me, alias, parameters);
        }

        public static string ToSetStatements<T>(this Expression<Func<T>> me, IDictionary<string, object> parameters)
        {
            return ToSetStatements(me, string.Empty, parameters);
        }

        public static string ToSetStatements<T>(this Expression<Func<T>> me, string alias, IDictionary<string, object> parameters)
        {
            if (!(me.Body is MemberInitExpression memberInitExpr)) throw new ArgumentException("Must be member initializer.");

            alias = string.IsNullOrEmpty(alias) ? alias : string.Concat("[", alias, "]");

            var sb = new StringBuilder();

            foreach (var binding in memberInitExpr.Bindings)
            {
                if (!(binding is MemberAssignment memberAssignment)) throw new ArgumentException("Must be member assignment.");

                if (Attribute.IsDefined(memberAssignment.Member, typeof(NotMappedAttribute)))
                {
                    throw new ArgumentException("Member can not applied [NotMapped].");
                }

                var columnAttribute = memberAssignment.Member.GetCustomAttribute<ColumnAttribute>();
                var parameterName = memberAssignment.Member.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var parameterType = ((PropertyInfo)memberAssignment.Member).PropertyType;

                if (parameters != null)
                {
                    SetParameter(memberAssignment.Member, ExtractConstant(memberAssignment.Expression), columnAttribute, parameters, null, out parameterName);
                }

                sb.AliasAppend($"[{columnName}] = {GenerateParameterStatement(parameterName, parameterType, parameters, null)}, ", alias);
            }

            sb.Remove(sb.Length - 2, 2);

            return sb.ToString();
        }

        public static string ToOrderAscending<T>(this Expression<Func<T, object>> me)
        {
            return ToOrderAscending(me, string.Empty);
        }

        public static string ToOrderAscending<T>(this Expression<Func<T, object>> me, string alias)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderAscending(memberExpr, GenerateAliasMap(me.Parameters, new[] { alias }));
        }

        public static string ToOrderDescending<T>(this Expression<Func<T, object>> me)
        {
            return ToOrderDescending(me, string.Empty);
        }

        public static string ToOrderDescending<T>(this Expression<Func<T, object>> me, string alias)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderDescending(memberExpr, GenerateAliasMap(me.Parameters, new[] { alias }));
        }

        public static string ToOrderAscending<T, TSecond>(this Expression<Func<T, TSecond, object>> me)
        {
            return ToOrderAscending(me, new string[] { });
        }

        public static string ToOrderAscending<T, TSecond>(this Expression<Func<T, TSecond, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderAscending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderDescending<T, TSecond>(this Expression<Func<T, TSecond, object>> me)
        {
            return ToOrderDescending(me, new string[] { });
        }

        public static string ToOrderDescending<T, TSecond>(this Expression<Func<T, TSecond, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderDescending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderAscending<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, object>> me)
        {
            return ToOrderAscending(me, new string[] { });
        }

        public static string ToOrderAscending<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderAscending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderDescending<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, object>> me)
        {
            return ToOrderDescending(me, new string[] { });
        }

        public static string ToOrderDescending<T, TSecond, TThird>(this Expression<Func<T, TSecond, TThird, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderDescending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, object>> me)
        {
            return ToOrderAscending(me, new string[] { });
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderAscending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, object>> me)
        {
            return ToOrderDescending(me, new string[] { });
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth>(this Expression<Func<T, TSecond, TThird, TFourth, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderDescending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> me)
        {
            return ToOrderAscending(me, new string[] { });
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderAscending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> me)
        {
            return ToOrderDescending(me, new string[] { });
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth, TFifth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderDescending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> me)
        {
            return ToOrderAscending(me, new string[] { });
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderAscending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> me)
        {
            return ToOrderDescending(me, new string[] { });
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth, TFifth, TSixth>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderDescending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> me)
        {
            return ToOrderAscending(me, new string[] { });
        }

        public static string ToOrderAscending<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderAscending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> me)
        {
            return ToOrderDescending(me, new string[] { });
        }

        public static string ToOrderDescending<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>> me, string[] aliases)
        {
            var memberExpr = ExtractMember(me.Body);

            return ToOrderDescending(memberExpr, GenerateAliasMap(me.Parameters, aliases));
        }

        private static IDictionary<string, string> GenerateAliasMap(IList<ParameterExpression> parameterExprs, string[] aliases)
        {
            var aliasMap = new Dictionary<string, string>();

            for (var i = 0; i < parameterExprs.Count; i++)
            {
                if (i < aliases.Length)
                {
                    var alias = aliases[i];

                    alias = string.IsNullOrEmpty(alias) ? alias : string.Concat("[", alias, "]");

                    aliasMap.Add(parameterExprs[i].Name, alias);
                }
                else
                {
                    aliasMap.Add(parameterExprs[i].Name, string.Empty);
                }
            }

            return aliasMap;
        }

        private static void ParseCondition(Expression expr, IDictionary<string, string> aliasMap, StringBuilder sb, IDictionary<string, object> parameters, IDictionary<string, string> parameterNames, List<PropertyInfo> involvedMembers)
        {
            var isNot = false;

            if (expr.NodeType == ExpressionType.Not)
            {
                isNot = true;
                expr = ((UnaryExpression)expr).Operand;
            }

            if (expr is BinaryExpression binaryExpr)
            {
                if (binaryExpr.NodeType == ExpressionType.AndAlso || binaryExpr.NodeType == ExpressionType.OrElse)
                {
                    sb.Append("(");

                    ParseCondition(binaryExpr.Left, aliasMap, sb, parameters, parameterNames, involvedMembers);

                    switch (binaryExpr.NodeType)
                    {
                        case ExpressionType.AndAlso:
                            sb.Append(") AND (");
                            break;

                        case ExpressionType.OrElse:
                            sb.Append(") OR (");
                            break;
                    }

                    ParseCondition(binaryExpr.Right, aliasMap, sb, parameters, parameterNames, involvedMembers);

                    sb.Append(")");
                }
                else
                {
                    var argumentExpr = binaryExpr.Right;

                    while (argumentExpr is UnaryExpression unaryExpr)
                    {
                        argumentExpr = unaryExpr.Operand; 
                    }

                    var operatorType = binaryExpr.NodeType;

                    BinaryExpression binaryLeft;

                    if (binaryExpr.Left.NodeType == ExpressionType.And || binaryExpr.Left.NodeType == ExpressionType.Or)
                    {
                        sb.Append("(");

                        ParseCondition(binaryExpr.Left, aliasMap, sb, parameters, parameterNames, involvedMembers);

                        sb.Append(")");

                        binaryLeft = (BinaryExpression)binaryExpr.Left;
                    }
                    else
                    {
                        binaryLeft = binaryExpr;
                    }

                    if (!(binaryLeft.Left is MemberExpression left))
                    {
                        switch (binaryLeft.Left)
                        {
                            case UnaryExpression unaryExpr when (left = unaryExpr.Operand as MemberExpression) != null: break;

                            case MethodCallExpression methodCallExpr when methodCallExpr.Method.GetFullName().EndsWith(".CompareTo") && (left = methodCallExpr.Object as MemberExpression) != null:
                                argumentExpr = methodCallExpr.Arguments[0];
                                break;

                            default: throw new ArgumentException("Left expression must be MemberExpression.");
                        }
                    }

                    if (!(left.Expression is ParameterExpression parameterExpr))
                    {
                        throw new ArgumentException("Parameter expression must be placed left.");
                    }

                    if (Attribute.IsDefined(left.Member, typeof(NotMappedAttribute)))
                    {
                        throw new ArgumentException("Member can not applied [NotMapped].");
                    }

                    var alias = aliasMap[parameterExpr.Name];
                    var columnAttribute = left.Member.GetCustomAttribute<ColumnAttribute>();
                    var parameterName = left.Member.Name;
                    var columnName = columnAttribute?.Name ?? parameterName;
                    var leftMember = (PropertyInfo)left.Member;
                    var parameterType = leftMember.PropertyType;

                    involvedMembers?.Add(leftMember);

                    if (argumentExpr is MemberExpression rightMemberExpr && rightMemberExpr.Expression is ParameterExpression rightParameterExpr)
                    {
                        var rightAlias = aliasMap[rightParameterExpr.Name];
                        var rightExprMember = (PropertyInfo)rightMemberExpr.Member;
                        var rightColumnAttribute = rightExprMember.GetCustomAttribute<ColumnAttribute>();
                        var rightColumnName = rightColumnAttribute?.Name ?? rightMemberExpr.Member.Name;

                        involvedMembers?.Add(rightExprMember);

                        if (binaryLeft != binaryExpr)
                        {
                            sb.Append($" {MapOperator(operatorType)} ");
                        }
                        else
                        {
                            sb.AliasAppend($"[{columnName}] {MapOperator(operatorType)} ", alias);
                        }

                        sb.AliasAppend($"[{rightColumnName}]", rightAlias);
                    }
                    else
                    {
                        if (parameters != null)
                        {
                            SetParameter(left.Member, ExtractConstant(argumentExpr), columnAttribute, parameters, parameterNames, out parameterName);
                        }

                        if (parameters != null && parameters[parameterName] == null)
                        {
                            if (binaryLeft != binaryExpr)
                            {
                                throw new SyntaxErrorException("Invalid NULL syntax.");
                            }

                            switch (operatorType)
                            {
                                case ExpressionType.Equal:
                                    sb.AliasAppend($"[{columnName}] IS NULL", alias);
                                    break;

                                case ExpressionType.NotEqual:
                                    sb.AliasAppend($"[{columnName}] IS NOT NULL", alias);
                                    break;

                                default: throw new ArgumentException("Invalid NodeType.");
                            }
                        }
                        else
                        {
                            if (binaryLeft != binaryExpr)
                            {
                                sb.Append($" {MapOperator(operatorType)} {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)}");
                            }
                            else
                            {
                                sb.AliasAppend($"[{columnName}] {MapOperator(operatorType)} {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)}", alias);
                            }
                        }
                    }
                }
            }
            else if (expr is MethodCallExpression methodCallExpr)
            {
                var methodFullName = methodCallExpr.Method.GetFullName();

                if (methodFullName.EndsWith(".Equals"))
                {
                    var memberExpr = (MemberExpression)methodCallExpr.Object;

                    if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
                    {
                        throw new ArgumentException("Member can not applied [NotMapped].");
                    }

                    var parameterExpr = (ParameterExpression)memberExpr.Expression;

                    var alias = aliasMap[parameterExpr.Name];
                    var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                    var parameterName = memberExpr.Member.Name;
                    var columnName = columnAttribute?.Name ?? parameterName;
                    var exprMember = (PropertyInfo)memberExpr.Member;
                    var parameterType = exprMember.PropertyType;

                    involvedMembers?.Add(exprMember);

                    if (parameters != null)
                    {
                        SetParameter(memberExpr.Member, ExtractConstant(methodCallExpr.Arguments[0]), columnAttribute, parameters, parameterNames, out parameterName);
                    }

                    sb.AliasAppend($"[{columnName}] {(isNot ? "<>" : "=")} {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)}", alias);
                }
                else if (methodFullName.IsLikeOperator())
                {
                    var memberExpr = (MemberExpression)methodCallExpr.Object;

                    if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
                    {
                        throw new ArgumentException("Member can not applied [NotMapped].");
                    }

                    var parameterExpr = (ParameterExpression)memberExpr.Expression;

                    var alias = aliasMap[parameterExpr.Name];
                    var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                    var parameterName = memberExpr.Member.Name;
                    var columnName = columnAttribute?.Name ?? parameterName;
                    var exprMember = (PropertyInfo)memberExpr.Member;
                    var parameterType = exprMember.PropertyType;

                    involvedMembers?.Add(exprMember);

                    if (parameters != null)
                    {
                        SetParameter(memberExpr.Member, ExtractConstant(methodCallExpr.Arguments[0]), columnAttribute, parameters, parameterNames, out parameterName);
                    }

                    if (methodFullName.Equals("System.String.Contains"))
                    {
                        sb.AliasAppend($"[{columnName}] {(isNot ? "NOT LIKE" : "LIKE")} '%' + {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)} + '%'", alias);
                    }
                    else if (methodFullName.Equals("System.String.StartsWith"))
                    {
                        sb.AliasAppend($"[{columnName}] {(isNot ? "NOT LIKE" : "LIKE")} {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)} + '%'", alias);
                    }
                    else if (methodFullName.Equals("System.String.EndsWith"))
                    {
                        sb.AliasAppend($"[{columnName}] {(isNot ? "NOT LIKE" : "LIKE")} '%' + {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)}", alias);
                    }
                }
                else if (methodFullName.EndsWith(".Includes"))
                {
                    var memberExpr = (MemberExpression)methodCallExpr.Arguments[0];

                    if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
                    {
                        throw new ArgumentException("Member can not applied [NotMapped].");
                    }

                    var parameterExpr = (ParameterExpression)memberExpr.Expression;

                    var alias = aliasMap[parameterExpr.Name];
                    var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                    var parameterName = memberExpr.Member.Name;
                    var columnName = columnAttribute?.Name ?? parameterName;
                    var exprMember = (PropertyInfo)memberExpr.Member;
                    var parameterType = exprMember.PropertyType;

                    involvedMembers?.Add(exprMember);

                    if (parameters != null)
                    {
                        SetParameter(memberExpr.Member, ExtractConstant(methodCallExpr.Arguments[1]), columnAttribute, parameters, parameterNames, out parameterName);
                    }

                    sb.Append($"{(isNot ? "NOT CONTAINS(" : "CONTAINS(")}");
                    sb.AliasAppend($"[{columnName}], {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)})", alias);
                }
                else if (methodFullName.EndsWith(".Contains"))
                {
                    var argumentExpr = methodCallExpr.Object == null
                                           ? (MemberExpression)methodCallExpr.Arguments[1]
                                           : (MemberExpression)methodCallExpr.Arguments[0];

                    if (Attribute.IsDefined(argumentExpr.Member, typeof(NotMappedAttribute)))
                    {
                        throw new ArgumentException("Member can not applied [NotMapped].");
                    }

                    var parameterExpr = (ParameterExpression)argumentExpr.Expression;

                    var alias = aliasMap[parameterExpr.Name];
                    var columnAttribute = argumentExpr.Member.GetCustomAttribute<ColumnAttribute>();
                    var columnName = columnAttribute?.Name ?? argumentExpr.Member.Name;
                    var argumentMember = (PropertyInfo)argumentExpr.Member;
                    var parameterType = argumentMember.PropertyType;

                    involvedMembers?.Add(argumentMember);

                    var array = ExtractArray(methodCallExpr.Object ?? methodCallExpr.Arguments[0]);

                    var any = false;

                    foreach (var item in array)
                    {
                        any = true;

                        if (parameters == null) throw new ArgumentException($"'{nameof(parameters)}' can not be null.");

                        SetParameter(argumentExpr.Member, item, columnAttribute, parameters, parameterNames, out var parameterName);

                        sb.AliasAppend(
                            isNot
                                ? $"[{columnName}] <> {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)} AND "
                                : $"[{columnName}] = {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)} OR ",
                            alias);
                    }

                    if (any)
                    {
                        if (isNot)
                        {
                            sb.Remove(sb.Length - 5, 5);
                        }
                        else
                        {
                            sb.Remove(sb.Length - 4, 4);
                        }
                    }
                    else
                    {
                        sb.Append("1 = 0");
                    }
                }
            }
            else if (expr is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression parameterExpr)
            {
                var alias = aliasMap[parameterExpr.Name];
                var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
                var parameterName = memberExpr.Member.Name;
                var columnName = columnAttribute?.Name ?? parameterName;
                var exprMember = (PropertyInfo)memberExpr.Member;
                var parameterType = exprMember.PropertyType;

                involvedMembers?.Add(exprMember);

                if (parameters != null && parameterType == typeof(bool))
                {
                    SetParameter(memberExpr.Member, !isNot, columnAttribute, parameters, parameterNames, out parameterName);
                }

                sb.AliasAppend($"[{columnName}] = {GenerateParameterStatement(parameterName, parameterType, parameters, parameterNames)}", alias);
            }
        }

        private static void SetParameter(MemberInfo member, object value, ColumnAttribute columnAttribute, IDictionary<string, object> parameters, IDictionary<string, string> parameterNames, out string parameterName)
        {
            if (parameterNames == null || !parameterNames.TryGetValue(member.Name, out parameterName))
            {
                parameterName = CreateUniqueParameterName(member.Name, parameters);
            }

            if (value != null && !string.IsNullOrEmpty(columnAttribute?.TypeName))
            {
                parameters[parameterName] = CreateDbString(
                    (string)value,
                    columnAttribute.TypeName,
                    member.GetCustomAttribute<StringLengthAttribute>()?.MaximumLength ?? -1);
            }
            else
            {
                parameters[parameterName] = value;
            }
        }

        private static string CreateUniqueParameterName(string memberName, IDictionary<string, object> parameters)
        {
            var index = 0;

            string parameterName;
            while (parameters.ContainsKey(parameterName = $"{memberName}_{index++}"))
            {
            }

            return parameterName;
        }

        private static object ExtractConstant(Expression expr)
        {
            if (expr == null) return null;

            if (expr is MemberExpression memberExpr)
            {
                if (memberExpr.Member.MemberType == MemberTypes.Field)
                {
                    if (memberExpr.Expression is MemberExpression)
                    {
                        return ((FieldInfo)memberExpr.Member).GetValue(ExtractConstant(memberExpr.Expression));
                    }
                    else
                    {
                        return ((FieldInfo)memberExpr.Member).GetValue((memberExpr.Expression as ConstantExpression)?.Value);
                    }
                }

                if (memberExpr.Member.MemberType == MemberTypes.Property)
                {
                    return ((PropertyInfo)memberExpr.Member).GetValue(ExtractConstant(memberExpr.Expression));
                }
            }

            if (expr is ConstantExpression constantExpr)
            {
                return constantExpr.Value;
            }

            if (expr is UnaryExpression unaryExpr)
            {
                return ExtractConstant(unaryExpr.Operand);
            }

            throw new ArgumentException("Right expression's node type must be Field or Property'");
        }

        private static DbString CreateDbString(string value, string typeName, int length)
        {
            switch (typeName.ToUpperInvariant())
            {
                case "VARCHAR": return new DbString { Value = value, Length = length, IsFixedLength = false, IsAnsi = true };
                case "CHAR": return new DbString { Value = value, Length = length, IsFixedLength = true, IsAnsi = true };
                case "NCHAR": return new DbString { Value = value, Length = length, IsFixedLength = true, IsAnsi = false };
                case "NVARCHAR":
                default: return new DbString { Value = value, Length = length, IsFixedLength = false, IsAnsi = false };
            }
        }

        private static IEnumerable ExtractArray(Expression expr)
        {
            switch (expr)
            {
                case NewArrayExpression newArrayExpr: return newArrayExpr.Expressions.Select(e => ExtractConstant(e)).ToArray();
                case MemberExpression arrayExpr: return (IEnumerable)ExtractConstant(arrayExpr);
                default: throw new ArgumentException("Must be a array variable or array initializer.");
            }
        }

        private static string MapOperator(ExpressionType exprType)
        {
            switch (exprType)
            {
                case ExpressionType.Equal: return "=";
                case ExpressionType.NotEqual: return "<>";
                case ExpressionType.GreaterThan: return ">";
                case ExpressionType.GreaterThanOrEqual: return ">=";
                case ExpressionType.LessThan: return "<";
                case ExpressionType.LessThanOrEqual: return "<=";
                case ExpressionType.And: return "&";
                case ExpressionType.Or: return "|";
                default: throw new ArgumentException("Invalid NodeType.");
            }
        }

        private static string MapSqlType(Type csharpType, ColumnAttribute columnAttribute)
        {
            if (columnAttribute != null && !string.IsNullOrEmpty(columnAttribute.TypeName))
            {
                if (columnAttribute.TypeName.Equals("varchar", StringComparison.OrdinalIgnoreCase)) return "VARCHAR(MAX)";
            }

            if (csharpType == typeof(string)) return "NVARCHAR(MAX)";
            if (csharpType == typeof(int)) return "INT";
            if (csharpType == typeof(long)) return "BIGINT";
            if (csharpType == typeof(decimal)) return "DECIMAL(38, 18)";
            if (csharpType == typeof(bool)) return "BIT";
            if (csharpType == typeof(float)) return "REAL";
            if (csharpType == typeof(double)) return "FLOAT";
            if (csharpType == typeof(short)) return "SMALLINT";
            if (csharpType == typeof(byte)) return "TINYINT";
            if (csharpType == typeof(sbyte)) return "SMALLINT";
            if (csharpType == typeof(ushort)) return "INT";
            if (csharpType == typeof(uint)) return "BIGINT";
            if (csharpType == typeof(ulong)) return "DECIMAL(20, 0)";
            if (csharpType == typeof(char)) return "NCHAR(1)";

            throw new ArgumentOutOfRangeException(nameof(csharpType));
        }

        private static IEnumerable<MemberExpression> GetMemberExpressions(Expression expr)
        {
            if (expr is NewExpression newExpr) return GetMemberExpressions(newExpr);

            if (expr is MemberInitExpression memberInitExpr)
            {
                return memberInitExpr.Bindings.Select(x => (MemberExpression)((MemberAssignment)x).Expression);
            }

            throw new ArgumentException("Selector must be a NewExpression or MemberInitExpression.");
        }

        private static IEnumerable<MemberExpression> GetMemberExpressions(NewExpression newExpr)
        {
            foreach (var argumentExpr in newExpr.Arguments)
            {
                if (argumentExpr is MemberExpression memberExpr) yield return memberExpr;

                if (argumentExpr is ParameterExpression parameterExpr)
                {
                    var properties = AllColumns.GetOrAdd(
                        parameterExpr.Type,
                        type => type.GetProperties().Where(p => !Attribute.IsDefined(p, typeof(NotMappedAttribute))).ToArray());

                    foreach (var property in properties)
                    {
                        yield return Expression.Property(parameterExpr, property.Name);
                    }
                }
            }
        }

        private static string GenerateParameterStatement(string parameterName, Type parameterType, IDictionary<string, object> parameters, IDictionary<string, string> parameterNames)
        {
            if (parameterNames != null && parameterNames.ContainsKey(parameterName)) return $"@{parameterNames[parameterName]}";

            if (parameters?[parameterName] != null)
            {
                parameterType = parameters[parameterName].GetType();
            }

            if (NumericTypes.Contains(parameterType)) return $"{{={parameterName}}}";

            return $"@{parameterName}";
        }

        private static MemberExpression ExtractMember(Expression expr)
        {
            switch (expr)
            {
                case UnaryExpression unaryExpr: return (MemberExpression)unaryExpr.Operand;
                case MemberExpression memberExpr: return memberExpr;
                default: throw new ArgumentException("Body expression must be MemberExpression.");
            }
        }

        private static string ToOrderAscending(MemberExpression memberExpr, IDictionary<string, string> aliasMap)
        {
            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var alias = aliasMap[parameterExpr.Name];
            var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
            var memberName = memberExpr.Member.Name;
            var columnName = columnAttribute?.Name ?? memberName;

            return string.IsNullOrEmpty(alias) ? $"[{memberName}] ASC" : $"{alias}.[{columnName}] ASC";
        }

        private static string ToOrderDescending(MemberExpression memberExpr, IDictionary<string, string> aliasMap)
        {
            if (Attribute.IsDefined(memberExpr.Member, typeof(NotMappedAttribute)))
            {
                throw new ArgumentException("Member can not applied [NotMapped].");
            }

            var parameterExpr = (ParameterExpression)memberExpr.Expression;

            var alias = aliasMap[parameterExpr.Name];
            var columnAttribute = memberExpr.Member.GetCustomAttribute<ColumnAttribute>();
            var memberName = memberExpr.Member.Name;
            var columnName = columnAttribute?.Name ?? memberName;

            return string.IsNullOrEmpty(alias) ? $"[{memberName}] DESC" : $"{alias}.[{columnName}] DESC";
        }
    }
}