﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class ExpressionExtension
    {
        public static string ToOrderExpressions<T>(this IEnumerable<(Expression<Func<T, object>>, Sortord)> me, string alias)
        {
            if (me == null) return string.Empty;
            if (!me.Any()) return string.Empty;

            var orderExpression = me.Select(
                o =>
                    {
                        var (expr, sortord) = o;

                        return sortord == Sortord.Descending ? expr.ToOrderDescending(alias) : expr.ToOrderAscending(alias);
                    });

            return string.Join(", ", orderExpression);
        }

        public static string ToOrderExpressions<T, TSecond>(this IEnumerable<(Expression<Func<T, TSecond, object>>, Sortord)> me, string[] aliases)
        {
            if (me == null) return string.Empty;
            if (!me.Any()) return string.Empty;

            var orderExpression = me.Select(
                o =>
                    {
                        var (expr, sortord) = o;

                        return sortord == Sortord.Descending ? expr.ToOrderDescending(aliases) : expr.ToOrderAscending(aliases);
                    });

            return string.Join(", ", orderExpression);
        }

        public static string ToOrderExpressions<T, TSecond, TThird>(this IEnumerable<(Expression<Func<T, TSecond, TThird, object>>, Sortord)> me, string[] aliases)
        {
            if (me == null) return string.Empty;
            if (!me.Any()) return string.Empty;

            var orderExpression = me.Select(
                o =>
                    {
                        var (expr, sortord) = o;

                        return sortord == Sortord.Descending ? expr.ToOrderDescending(aliases) : expr.ToOrderAscending(aliases);
                    });

            return string.Join(", ", orderExpression);
        }

        public static string ToOrderExpressions<T, TSecond, TThird, TFourth>(this IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, object>>, Sortord)> me, string[] aliases)
        {
            if (me == null) return string.Empty;
            if (!me.Any()) return string.Empty;

            var orderExpression = me.Select(
                o =>
                    {
                        var (expr, sortord) = o;

                        return sortord == Sortord.Descending ? expr.ToOrderDescending(aliases) : expr.ToOrderAscending(aliases);
                    });

            return string.Join(", ", orderExpression);
        }

        public static string ToOrderExpressions<T, TSecond, TThird, TFourth, TFifth>(this IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, object>>, Sortord)> me, string[] aliases)
        {
            if (me == null) return string.Empty;
            if (!me.Any()) return string.Empty;

            var orderExpression = me.Select(
                o =>
                    {
                        var (expr, sortord) = o;

                        return sortord == Sortord.Descending ? expr.ToOrderDescending(aliases) : expr.ToOrderAscending(aliases);
                    });

            return string.Join(", ", orderExpression);
        }

        public static string ToOrderExpressions<T, TSecond, TThird, TFourth, TFifth, TSixth>(this IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, object>>, Sortord)> me, string[] aliases)
        {
            if (me == null) return string.Empty;
            if (!me.Any()) return string.Empty;

            var orderExpression = me.Select(
                o =>
                    {
                        var (expr, sortord) = o;

                        return sortord == Sortord.Descending ? expr.ToOrderDescending(aliases) : expr.ToOrderAscending(aliases);
                    });

            return string.Join(", ", orderExpression);
        }

        public static string ToOrderExpressions<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>(this IEnumerable<(Expression<Func<T, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, object>>, Sortord)> me, string[] aliases)
        {
            if (me == null) return string.Empty;
            if (!me.Any()) return string.Empty;

            var orderExpression = me.Select(
                o =>
                    {
                        var (expr, sortord) = o;

                        return sortord == Sortord.Descending ? expr.ToOrderDescending(aliases) : expr.ToOrderAscending(aliases);
                    });

            return string.Join(", ", orderExpression);
        }
    }
}